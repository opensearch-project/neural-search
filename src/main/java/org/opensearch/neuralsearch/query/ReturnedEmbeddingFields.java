/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.core.index.Index;
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;
import org.opensearch.search.SearchService;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.search.fetch.subphase.FieldAndFormat;

import lombok.extern.log4j.Log4j2;

/**
 * How much embedding payload would the fused fast path fetch beyond what round 2 fetches, and is that worth the round it
 * saves?
 *
 * <p>The fast path assembles the page from the legs, so each leg fetches its whole window — {@code legs × window_size}
 * documents — where round 2 fetched only the page ({@code size} documents). For small documents that is a clear win. For
 * documents whose {@code _source} carries a dense vector it is a trade: measured paired on a 768-dimension index with the
 * vector in {@code _source}, the extra ~1.9 MB of a {@code size:10, window:100} request cost ~18 ms against a round-2 saving
 * of ~13 ms (the fast path lost by 5 ms), while the extra ~1 MB of a {@code size:100} request cost ~4 ms and the fast path
 * still won by 9. What decides it is the <b>extra</b> fetch volume, {@code (legs × window − size) × bytes per document},
 * against the round saved — not the presence of a vector, and not {@code window} or {@code size} alone.
 *
 * <p>Both terms are knowable before the legs run. The bytes per document are estimated from the mapping: for each
 * {@code knn_vector} the page would carry, {@code dimension} times the width of one serialized value (~10 bytes for a
 * float rendered as JSON text, ~4 for a byte vector; a vector without a declared dimension is taken as 1024-wide), and
 * a fixed ~4 KB for a {@code rank_features} field. A field "would be carried" when it survives the same filters core
 * applies — the mapping-level {@code _source.includes/excludes} and the request-level {@code _source}
 * includes/excludes, both {@link XContentMapValues#filter} — or is named by {@code fields}/{@code docvalue_fields}.
 * {@code _source: false} with no field patterns costs no lookup at all. The rewrite refuses the fast path when the extra
 * volume exceeds {@link #FAST_PATH_EXTRA_FETCH_BUDGET_BYTES}. This is a predictor of latency, not a correctness
 * condition — both paths return the same page — so an estimate that is off costs milliseconds only.
 *
 * <p>Derived source (k-NN {@code index.knn.derived_source.enabled}) is deliberately <b>not</b> consulted: it strips the
 * vector from the stored {@code _source} but re-injects it on read ({@code DerivedSourceVectorTransformer#injectVectors}),
 * so the fetched document is just as large. Only an include/exclude that drops the field changes the volume.
 *
 * <p>Fails closed: an index that cannot be resolved, is gone from the state, or has a mapping that cannot be parsed
 * answers "too large", and the request takes the two-round path it always had. The mapping walk is cached per index and
 * mapping version — {@code MappingMetadata#sourceAsMap} parses the compressed mapping on every call, which would cost
 * more than the round the fast path saves.
 */
@Log4j2
final class ReturnedEmbeddingFields {

    /** Field types whose values are large by construction and never needed to render a page. */
    static final Set<String> EMBEDDING_FIELD_TYPES = Set.of("knn_vector", "rank_features");

    /**
     * The extra fetch volume, beyond the page round 2 would have fetched, above which the fast path is refused. Measured
     * crossover on a serverless fleet: ~1.3–3 MB of extra documents cost as much as the round saved; 1 MB refuses well
     * before it while keeping a {@code size ≈ window} request with 768-dim vectors (~0.77 MB extra) on the fast path.
     */
    static final long FAST_PATH_EXTRA_FETCH_BUDGET_BYTES = 1L << 20;
    /** A float rendered in JSON ({@code 0.12345678,}) is about this many bytes. */
    static final long FLOAT_VECTOR_BYTES_PER_DIMENSION = 10;
    /** A byte value rendered in JSON ({@code -12,}) is about this many bytes; binary vectors are declared in bits. */
    static final long BYTE_VECTOR_BYTES_PER_DIMENSION = 4;
    static final long BINARY_VECTOR_BYTES_PER_DIMENSION = 1;
    /** A {@code knn_vector} without a declared dimension (model-bound) is taken as this wide. */
    static final long UNKNOWN_DIMENSION = 1024;
    /** A sparse embedding ({@code rank_features}): a few hundred tokens with weights. */
    static final long RANK_FEATURES_BYTES = 4096;

    private static final int CACHE_CAPACITY = 4096;

    /** No filter: the document as laid out. */
    private static final Function<Map<String, ?>, Map<String, Object>> PASS_THROUGH = document -> new HashMap<>(document);

    /** What a mapping says, resolved once per mapping version: its embedding paths and its own {@code _source} filter. */
    private record MappingFacts(long mappingVersion, Map<String, Long> embeddingBytes, Function<
        Map<String, ?>,
        Map<String, Object>> sourceFilter) {
    }

    private static final Map<Index, MappingFacts> CACHE = new ConcurrentHashMap<>();

    private ReturnedEmbeddingFields() {}

    /**
     * True when the fast path would fetch more than {@link #FAST_PATH_EXTRA_FETCH_BUDGET_BYTES} of embedding payload
     * beyond the page round 2 fetches — {@code (legs × window − size) × bytes per document} — or when that cannot be
     * established.
     */
    static boolean fastPathFetchExceedsBudget(final SearchRequest request, final int legCount, final int windowSize) {
        long bytesPerDocument = estimatedEmbeddingBytesPerDocument(request);
        if (bytesPerDocument == 0) {
            return false;
        }
        if (bytesPerDocument == Long.MAX_VALUE) {
            return true;
        }
        SearchSourceBuilder source = request.source();
        int size = Objects.isNull(source) || source.size() < 0 ? SearchService.DEFAULT_SIZE : source.size();
        long extraDocuments = Math.max(0L, (long) legCount * windowSize - size);
        return extraDocuments * bytesPerDocument > FAST_PATH_EXTRA_FETCH_BUDGET_BYTES;
    }

    /**
     * Estimated bytes of embedding payload one returned document would carry for this request: {@code 0} when none would,
     * {@link Long#MAX_VALUE} when that cannot be established (fail closed), else the sum over the embedding fields that
     * survive the request's and the mappings' fetch filters, taking the widest estimate across the targeted indices.
     */
    static long estimatedEmbeddingBytesPerDocument(final SearchRequest request) {
        SearchSourceBuilder source = request.source();
        FetchSourceContext fetchSource = Objects.isNull(source) ? null : source.fetchSource();
        boolean sourceOn = Objects.isNull(fetchSource) || fetchSource.fetchSource();
        List<String> fieldPatterns = fieldPatterns(source);
        if (sourceOn == false && fieldPatterns.isEmpty()) {
            return 0;
        }
        List<IndexMetadata> indices;
        try {
            indices = NeuralSearchClusterUtil.instance().getIndexMetadataList(request);
        } catch (Exception e) {
            log.debug("fused fast path: cannot resolve the request's indices on the coordinator, taking the two-round path", e);
            return Long.MAX_VALUE;
        }
        if (indices.isEmpty()) {
            return Long.MAX_VALUE;
        }
        Function<Map<String, ?>, Map<String, Object>> requestFilter = Objects.isNull(fetchSource) ? PASS_THROUGH : fetchSource.getFilter();
        long widest = 0;
        for (IndexMetadata index : indices) {
            if (Objects.isNull(index)) {
                // resolved a moment ago, gone from the state now: cannot be established, take the two-round path
                return Long.MAX_VALUE;
            }
            MappingFacts facts;
            try {
                facts = factsFor(index);
            } catch (Exception e) {
                log.debug("fused fast path: cannot read the mapping of [{}], taking the two-round path", index.getIndex(), e);
                return Long.MAX_VALUE;
            }
            if (Objects.isNull(facts) || facts.embeddingBytes().isEmpty()) {
                continue;
            }
            Map<String, Object> carried = sourceOn ? carriedBySource(facts, requestFilter) : Collections.emptyMap();
            long bytes = 0;
            for (Map.Entry<String, Long> field : facts.embeddingBytes().entrySet()) {
                boolean inSource = Objects.nonNull(XContentMapValues.extractValue(field.getKey(), carried));
                boolean inFields = fieldPatterns.stream().anyMatch(pattern -> Regex.simpleMatch(pattern, field.getKey()));
                if (inSource || inFields) {
                    bytes += field.getValue();
                }
            }
            widest = Math.max(widest, bytes);
        }
        return widest;
    }

    /** Requested {@code fields} and {@code docvalue_fields} patterns; {@code stored_fields} cannot name an embedding. */
    private static List<String> fieldPatterns(final SearchSourceBuilder source) {
        if (Objects.isNull(source)) {
            return Collections.emptyList();
        }
        List<String> patterns = new ArrayList<>();
        for (List<FieldAndFormat> list : List.of(
            Objects.requireNonNullElse(source.fetchFields(), Collections.<FieldAndFormat>emptyList()),
            Objects.requireNonNullElse(source.docValueFields(), Collections.<FieldAndFormat>emptyList())
        )) {
            for (FieldAndFormat fieldAndFormat : list) {
                patterns.add(fieldAndFormat.field);
            }
        }
        return patterns;
    }

    /**
     * The embedding paths laid out as the nested map they would form in a document, put through the mapping-level and
     * then the request-level {@code _source} filter — the same {@link XContentMapValues#filter} core uses in both places,
     * so wildcards, dotted paths and object nesting behave exactly as they do for the real document. What survives is what
     * the page would carry.
     */
    private static Map<String, Object> carriedBySource(
        final MappingFacts facts,
        final Function<Map<String, ?>, Map<String, Object>> requestFilter
    ) {
        Map<String, Object> document = new HashMap<>();
        for (String path : facts.embeddingBytes().keySet()) {
            put(document, path.split("\\."), 0);
        }
        return requestFilter.apply(facts.sourceFilter().apply(document));
    }

    @SuppressWarnings("unchecked")
    private static void put(final Map<String, Object> into, final String[] parts, final int depth) {
        String key = parts[depth];
        if (depth == parts.length - 1) {
            into.put(key, Boolean.TRUE);
            return;
        }
        Object child = into.get(key);
        if ((child instanceof Map) == false) {
            child = new HashMap<String, Object>();
            into.put(key, child);
        }
        put((Map<String, Object>) child, parts, depth + 1);
    }

    private static MappingFacts factsFor(final IndexMetadata index) {
        MappingMetadata mapping = index.mapping();
        if (Objects.isNull(mapping)) {
            return null;
        }
        MappingFacts cached = CACHE.get(index.getIndex());
        if (Objects.nonNull(cached) && cached.mappingVersion() == index.getMappingVersion()) {
            return cached;
        }
        MappingFacts facts = read(mapping, index.getMappingVersion());
        if (CACHE.size() >= CACHE_CAPACITY) {
            CACHE.clear();
        }
        CACHE.put(index.getIndex(), facts);
        return facts;
    }

    @SuppressWarnings("unchecked")
    private static MappingFacts read(final MappingMetadata mapping, final long mappingVersion) {
        Map<String, Object> root = mapping.sourceAsMap();
        Map<String, Long> bytesByPath = new LinkedHashMap<>();
        Object properties = root.get("properties");
        if (properties instanceof Map) {
            collect((Map<String, Object>) properties, "", bytesByPath);
        }
        Function<Map<String, ?>, Map<String, Object>> sourceFilter = PASS_THROUGH;
        Object sourceSpec = root.get("_source");
        if (sourceSpec instanceof Map<?, ?> spec) {
            if (Boolean.FALSE.equals(spec.get("enabled"))) {
                // no _source at all: nothing is carried, whatever the paths say
                sourceFilter = document -> Collections.emptyMap();
            } else {
                String[] includes = patterns(spec.get("includes"));
                String[] excludes = patterns(spec.get("excludes"));
                if (includes.length > 0 || excludes.length > 0) {
                    sourceFilter = XContentMapValues.filter(includes, excludes, true);
                }
            }
        }
        return new MappingFacts(mappingVersion, Collections.unmodifiableMap(bytesByPath), sourceFilter);
    }

    @SuppressWarnings("unchecked")
    private static void collect(final Map<String, Object> properties, final String prefix, final Map<String, Long> into) {
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            if ((entry.getValue() instanceof Map) == false) {
                continue;
            }
            Map<String, Object> field = (Map<String, Object>) entry.getValue();
            String path = prefix.isEmpty() ? entry.getKey() : prefix + "." + entry.getKey();
            Object type = field.get("type");
            if (type instanceof String typeName && EMBEDDING_FIELD_TYPES.contains(typeName)) {
                into.put(path, estimatedBytes(typeName, field));
            }
            Object children = field.get("properties");
            if (children instanceof Map) {
                collect((Map<String, Object>) children, path, into);
            }
        }
    }

    /** Bytes one document's value of this field adds to the fetched {@code _source}, from the mapping's declaration. */
    static long estimatedBytes(final String type, final Map<String, Object> field) {
        if ("rank_features".equals(type)) {
            return RANK_FEATURES_BYTES;
        }
        long dimension = field.get("dimension") instanceof Number declared ? declared.longValue() : UNKNOWN_DIMENSION;
        String dataType = field.get("data_type") instanceof String declared ? declared : "float";
        long perDimension = switch (dataType) {
            case "byte" -> BYTE_VECTOR_BYTES_PER_DIMENSION;
            case "binary" -> BINARY_VECTOR_BYTES_PER_DIMENSION;
            default -> FLOAT_VECTOR_BYTES_PER_DIMENSION;
        };
        return dimension * perDimension;
    }

    private static String[] patterns(final Object value) {
        if (value instanceof String single) {
            return new String[] { single };
        }
        if (value instanceof List<?> list) {
            return list.stream().map(String::valueOf).toArray(String[]::new);
        }
        return new String[0];
    }

    /** Test hook: forget every cached mapping. */
    static void clearCache() {
        CACHE.clear();
    }
}
