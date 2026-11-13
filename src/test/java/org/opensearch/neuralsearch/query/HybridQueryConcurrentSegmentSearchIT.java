/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import lombok.SneakyThrows;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.junit.After;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import org.opensearch.neuralsearch.settings.NeuralSearchSettings;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.opensearch.neuralsearch.util.TestUtils.getTotalHits;

/**
 * A hybrid query must return the same response with concurrent segment search as without it
 * (https://github.com/opensearch-project/neural-search/issues/2091): every request runs once with
 * {@code search.concurrent_segment_search.mode: none} and once with {@code all} and four slices, on shards that keep at least six
 * segments (merging is held back and the per-shard segment count and the profiled slice count are asserted before the concurrent
 * arm), and the two responses must have the same hits (ids, order, sort values, collapse values), total and aggregations. Scores
 * and max score are compared too, except for field-sorted requests, where the sort collectors make them depend on how segments
 * are grouped into collectors (see {@link Expect#FIELD_SORTED_HITS}). Segments stay far below 4,096 docs, so the hybrid bulk
 * scorer never skips docs and totals are comparable.
 */
public class HybridQueryConcurrentSegmentSearchIT extends BaseNeuralSearchIT {

    private static final String CONCURRENT_SEGMENT_SEARCH_MODE = "search.concurrent_segment_search.mode";
    private static final String CONCURRENT_SEGMENT_SEARCH_MAX_SLICE_COUNT = "search.concurrent.max_slice_count";
    private static final int MAX_SLICE_COUNT = 4;
    private static final String INDEX_ONE_SHARD = "test-hybrid-css-one-shard";
    private static final String INDEX_THREE_SHARDS = "test-hybrid-css-three-shards";
    private static final String MIN_MAX_PIPELINE = "test-hybrid-css-min-max";
    private static final String RRF_PIPELINE = "test-hybrid-css-rrf";
    private static final String TEXT_FIELD = "text";
    private static final String VECTOR_FIELD = "vec";
    private static final String GROUP_FIELD = "group";
    private static final String BUCKET_FIELD = "bucket";
    private static final String PRICE_FIELD = "price";
    private static final String RANK_FIELD = "rank";
    private static final int BATCHES = 6;
    private static final int DOCS_PER_BATCH = 40;
    private static final int PAGINATION_DEPTH = 2;
    private static final String END_OF_RESULTS = "Reached end of search result, increase pagination_depth value to see more results";

    @After
    public void resetConcurrentSegmentSearch() {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_MODE, null);
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_MAX_SLICE_COUNT, null);
    }

    @SneakyThrows
    public void testHybridQuery_whenConcurrentSegmentSearch_thenSameResponseAsWithout_onOneShard() {
        createIndexWithSegmentsPerShard(INDEX_ONE_SHARD, 1);
        assertSameResponsesInBothModes(INDEX_ONE_SHARD, shapes(1));
        assertDistinctGroupsCollapseSucceedsInBothModes(INDEX_ONE_SHARD);
    }

    @SneakyThrows
    public void testHybridQuery_whenConcurrentSegmentSearch_thenSameResponseAsWithout_onThreeShards() {
        createIndexWithSegmentsPerShard(INDEX_THREE_SHARDS, 3);
        assertSameResponsesInBothModes(INDEX_THREE_SHARDS, shapes(3));
        assertDistinctGroupsCollapseSucceedsInBothModes(INDEX_THREE_SHARDS);
    }

    /** What a request must return in both modes: hits, the end of results error, or anything as long as it is the same. */
    private enum Expect {
        HITS,
        /**
         * Hits sorted by a field, compared without the max score: the sort collectors skip a doc for the later sub-queries once it is
         * not competitive for an earlier full queue, and keep the score of the entry a competitive hit replaces, so their sub-query
         * lists and scores, and with them the max score, depend on how segments are grouped into collectors (pre-existing, both
         * modes). Hit ids, order and sort values do not.
         */
        FIELD_SORTED_HITS,
        END_OF_RESULTS,
        SAME
    }

    /** One request shape: what to send and what both modes must agree on. */
    private record Shape(String name, String pipeline, Map<String, Object> body, Expect expect) {
    }

    private List<Shape> shapes(final int numberOfShards) {
        final List<Shape> shapes = new ArrayList<>();
        for (String pipeline : List.of(MIN_MAX_PIPELINE, RRF_PIPELINE)) {
            for (int size : List.of(1, 5)) {
                shapes.add(shape("two match legs", pipeline, body(hybrid(match("alpha"), match("beta")), "size", size)));
                shapes.add(shape("match and knn legs", pipeline, body(hybrid(match("alpha"), knn()), "size", size)));
                shapes.add(
                    shape(
                        "keyword collapse",
                        pipeline,
                        body(
                            hybrid(match("alpha"), match("beta")),
                            "size",
                            size,
                            "collapse",
                            collapse(GROUP_FIELD),
                            "track_total_hits",
                            true
                        )
                    )
                );
            }
        }
        // the rescore window is smaller than every sub-query's hits on a slice
        shapes.add(
            shape(
                "rescore",
                MIN_MAX_PIPELINE,
                body(
                    hybrid(match("alpha"), match("beta")),
                    "size",
                    5,
                    "rescore",
                    Map.of("window_size", 3, "query", Map.of("rescore_query", match("gamma")))
                )
            )
        );
        // a page that pagination_depth, not size, bounds: each shard keeps 20 hits per sub-query and the second page is served
        shapes.add(
            shape(
                "second page within pagination_depth",
                MIN_MAX_PIPELINE,
                body(paginatedHybrid(20, match("alpha"), match("beta")), "size", 5, "from", 5)
            )
        );
        // post_filter wraps the hybrid collector in core's FilteredCollector; the filter keeps about half of the docs
        shapes.add(
            shape(
                "post_filter",
                MIN_MAX_PIPELINE,
                body(
                    hybrid(match("alpha"), match("beta")),
                    "size",
                    5,
                    "post_filter",
                    Map.of("range", Map.of(PRICE_FIELD, Map.of("lte", 5)))
                )
            )
        );
        final List<Object> priceThenRank = List.of(order(PRICE_FIELD, "desc"), order(RANK_FIELD, "asc"));
        shapes.add(
            new Shape(
                "field sort",
                MIN_MAX_PIPELINE,
                body(hybrid(match("alpha"), match("beta")), "size", 5, "sort", priceThenRank),
                Expect.FIELD_SORTED_HITS
            )
        );
        shapes.add(
            new Shape(
                "field sort with search_after",
                MIN_MAX_PIPELINE,
                body(hybrid(match("alpha"), match("beta")), "size", 5, "sort", priceThenRank, "search_after", List.of(8, 50)),
                Expect.FIELD_SORTED_HITS
            )
        );
        shapes.add(shape("min_score", MIN_MAX_PIPELINE, body(hybrid(match("alpha"), knn()), "size", 5, "min_score", 0.5)));
        // beyond the combined hits of every shard: two hits per sub-query and shard, so at most four docs per shard
        shapes.add(
            new Shape(
                "from beyond pagination_depth",
                MIN_MAX_PIPELINE,
                body(
                    paginatedHybrid(PAGINATION_DEPTH, match("alpha"), match("beta")),
                    "size",
                    5,
                    "from",
                    2 * PAGINATION_DEPTH * numberOfShards + 1
                ),
                Expect.END_OF_RESULTS
            )
        );
        shapes.add(
            shape(
                "numeric collapse",
                MIN_MAX_PIPELINE,
                body(hybrid(match("alpha"), match("beta")), "size", 5, "collapse", collapse(BUCKET_FIELD), "track_total_hits", true)
            )
        );
        shapes.add(
            shape(
                "collapse sorted by score then field",
                MIN_MAX_PIPELINE,
                body(
                    hybrid(match("alpha"), match("beta")),
                    "size",
                    5,
                    "collapse",
                    collapse(GROUP_FIELD),
                    "sort",
                    List.of(order("_score", "desc"), order(PRICE_FIELD, "asc")),
                    "track_total_hits",
                    true
                )
            )
        );
        shapes.add(
            shape(
                "collapse sorted by a field",
                MIN_MAX_PIPELINE,
                body(
                    hybrid(match("alpha"), match("beta")),
                    "size",
                    5,
                    "collapse",
                    collapse(GROUP_FIELD),
                    "sort",
                    List.of(order(PRICE_FIELD, "desc")),
                    "track_total_hits",
                    true
                )
            )
        );
        shapes.add(
            shape(
                "collapse with a terms aggregation",
                RRF_PIPELINE,
                body(
                    hybrid(match("alpha"), match("beta")),
                    "size",
                    5,
                    "collapse",
                    collapse(GROUP_FIELD),
                    "aggs",
                    Map.of("groups", Map.of("terms", Map.of("field", GROUP_FIELD))),
                    "track_total_hits",
                    true
                )
            )
        );
        // more pages than there are groups
        shapes.add(
            new Shape(
                "collapse from beyond the groups",
                MIN_MAX_PIPELINE,
                body(
                    paginatedHybrid(10, match("alpha"), match("beta")),
                    "size",
                    5,
                    "from",
                    50,
                    "collapse",
                    collapse(GROUP_FIELD),
                    "track_total_hits",
                    true
                ),
                Expect.SAME
            )
        );
        return shapes;
    }

    private void assertSameResponsesInBothModes(final String index, final List<Shape> shapes) {
        setConcurrentSegmentSearch(false);
        final List<Map<String, Object>> withoutConcurrentSearch = new ArrayList<>();
        for (Shape shape : shapes) {
            withoutConcurrentSearch.add(search(index, shape));
        }
        setConcurrentSegmentSearch(true);
        // the concurrent arm is meaningful only if every shard still has several segments and core really splits them into slices
        assertEveryShardHasSeveralSegments(index);
        assertConcurrentSearchUsesSeveralSlices(index);
        // every difference of every shape, so one run shows the whole picture
        final List<String> differences = new ArrayList<>();
        for (int i = 0; i < shapes.size(); i++) {
            final Shape shape = shapes.get(i);
            final Map<String, Object> expected = withoutConcurrentSearch.get(i);
            final Map<String, Object> actual = search(index, shape);
            final String message = String.format(Locale.ROOT, "[%s] %s", index, shape.name());
            if (shape.expect() == Expect.END_OF_RESULTS) {
                compare(differences, message + " without concurrent search", Map.of("status", 400, "reason", END_OF_RESULTS), expected);
            }
            if (expected.containsKey("status") || actual.containsKey("status")) {
                if (shape.expect() == Expect.HITS || shape.expect() == Expect.FIELD_SORTED_HITS) {
                    differences.add(message + " failed: " + expected + " / " + actual);
                }
                compare(differences, message, expected, actual);
                continue;
            }
            compare(differences, message + " total", getTotalHits(expected), getTotalHits(actual));
            if (shape.expect() != Expect.FIELD_SORTED_HITS) {
                compare(differences, message + " max score", hitsOf(expected).get("max_score"), hitsOf(actual).get("max_score"));
            }
            compare(differences, message + " hits", hitSummaries(expected), hitSummaries(actual));
            compare(differences, message + " aggregations", expected.get("aggregations"), actual.get("aggregations"));
            if ((shape.expect() == Expect.HITS || shape.expect() == Expect.FIELD_SORTED_HITS) && hitSummaries(expected).isEmpty()) {
                differences.add(message + " has no hits");
            }
        }
        assertTrue(String.join("\n", differences), differences.isEmpty());
    }

    private static void compare(final List<String> differences, final String message, final Object expected, final Object actual) {
        if (Objects.equals(expected, actual) == false) {
            differences.add(String.format(Locale.ROOT, "%s%n  without: %s%n  with:    %s", message, expected, actual));
        }
    }

    /** Distinct-groups collapse keeps today's per-collector fold under concurrent segment search: no parity asserted yet. */
    @SneakyThrows
    private void assertDistinctGroupsCollapseSucceedsInBothModes(final String index) {
        updateIndexSettings(index, Settings.builder().put(NeuralSearchSettings.HYBRID_COLLAPSE_DISTINCT_GROUPS_ENABLED.getKey(), true));
        final Shape shape = shape(
            "distinct groups collapse",
            MIN_MAX_PIPELINE,
            body(hybrid(match("alpha"), match("beta")), "size", 5, "collapse", collapse(GROUP_FIELD), "track_total_hits", true)
        );
        for (boolean concurrent : List.of(false, true)) {
            setConcurrentSegmentSearch(concurrent);
            final Map<String, Object> response = search(index, shape);
            assertFalse(shape.name() + " failed: " + response, response.containsKey("status"));
            assertFalse(shape.name() + " has no hits", hitSummaries(response).isEmpty());
        }
    }

    @SneakyThrows
    private void createIndexWithSegmentsPerShard(final String index, final int numberOfShards) {
        final XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("settings")
            .field("number_of_shards", numberOfShards)
            .field("number_of_replicas", 0)
            .field("refresh_interval", "-1")
            // keep the one-segment-per-batch layout: no background merge may fold the segments into one before the concurrent arm
            .field("index.merge.policy.segments_per_tier", 2 * BATCHES)
            .field("index.merge.policy.floor_segment", "1kb")
            .field("index.knn", true)
            .endObject()
            .startObject("mappings")
            .startObject("properties")
            .startObject(TEXT_FIELD)
            .field("type", "text")
            .endObject()
            .startObject(VECTOR_FIELD)
            .field("type", "knn_vector")
            .field("dimension", 3)
            .startObject("method")
            .field("name", "hnsw")
            .field("space_type", "l2")
            .field("engine", "lucene")
            .endObject()
            .endObject()
            .startObject(GROUP_FIELD)
            .field("type", "keyword")
            .endObject()
            .startObject(BUCKET_FIELD)
            .field("type", "integer")
            .endObject()
            .startObject(PRICE_FIELD)
            .field("type", "integer")
            .endObject()
            .startObject(RANK_FIELD)
            .field("type", "integer")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        createIndexWithConfiguration(index, mapping.toString(), null);
        // one refresh per batch: every shard gets one segment per batch
        for (int batch = 0; batch < BATCHES; batch++) {
            final StringBuilder bulk = new StringBuilder();
            for (int doc = batch * DOCS_PER_BATCH; doc < (batch + 1) * DOCS_PER_BATCH; doc++) {
                bulk.append(String.format(Locale.ROOT, "{\"index\":{\"_index\":\"%s\",\"_id\":\"%d\"}}\n", index, doc));
                bulk.append(XContentFactory.jsonBuilder().map(document(doc)).toString()).append('\n');
            }
            final Request request = new Request("POST", "/_bulk");
            request.addParameter("refresh", "true");
            request.setJsonEntity(bulk.toString());
            final Response response = client().performRequest(request);
            assertFalse(EntityUtils.toString(response.getEntity()).contains("\"errors\":true"));
        }
        assertEveryShardHasSeveralSegments(index);
        createSearchPipeline(MIN_MAX_PIPELINE, "min_max", "arithmetic_mean", Map.of());
        createRRFSearchPipeline(RRF_PIPELINE, List.of(), false);
    }

    /**
     * Scripted docs: "alpha" in every second doc and "beta" in two of three, with term counts and lengths that repeat, so both
     * match legs have many hits with score ties; vectors repeat too. Five percent of the docs have no group.
     */
    private static Map<String, Object> document(final int doc) {
        final StringBuilder text = new StringBuilder("delta");
        for (int i = 0; doc % 2 == 0 && i < 1 + (doc / 2) % 3; i++) {
            text.append(" alpha");
        }
        for (int i = 0; doc % 3 != 0 && i < 1 + doc % 4; i++) {
            text.append(" beta");
        }
        for (int i = 0; i < doc % 5; i++) {
            text.append(" gamma");
        }
        final Map<String, Object> document = new LinkedHashMap<>();
        document.put(TEXT_FIELD, text.toString());
        document.put(VECTOR_FIELD, List.of((float) (doc % 7), (float) (doc % 11), (float) (doc % 13)));
        if (doc % 20 != 19) {
            document.put(GROUP_FIELD, "g" + doc % 6);
        }
        document.put(BUCKET_FIELD, doc % 4);
        document.put(PRICE_FIELD, (doc * 7) % 11);
        document.put(RANK_FIELD, doc);
        return document;
    }

    private void setConcurrentSegmentSearch(final boolean concurrent) {
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_MODE, concurrent ? "all" : "none");
        updateClusterSettings(CONCURRENT_SEGMENT_SEARCH_MAX_SLICE_COUNT, concurrent ? MAX_SLICE_COUNT : null);
    }

    /** Every shard of the index still holds at least BATCHES segments, so the slice merge has several lists to merge per shard. */
    @SneakyThrows
    private void assertEveryShardHasSeveralSegments(final String index) {
        final Request request = new Request("GET", "/_cat/segments/" + index);
        request.addParameter("h", "shard");
        final Map<String, Integer> segmentsPerShard = new LinkedHashMap<>();
        for (String line : EntityUtils.toString(client().performRequest(request).getEntity()).split("\n")) {
            if (line.isBlank() == false) {
                segmentsPerShard.merge(line.trim(), 1, Integer::sum);
            }
        }
        assertFalse("no segments reported for " + index, segmentsPerShard.isEmpty());
        for (Map.Entry<String, Integer> shard : segmentsPerShard.entrySet()) {
            assertTrue(
                String.format(
                    Locale.ROOT,
                    "[%s] shard %s has %d segments, fewer than %d",
                    index,
                    shard.getKey(),
                    shard.getValue(),
                    BATCHES
                ),
                shard.getValue() >= BATCHES
            );
        }
    }

    /**
     * With {@code mode: all} and four slices, core splits every shard's segments into several slices: the profiled hybrid collector
     * of each shard must report more than one slice, otherwise the concurrent arm would run the same single-collector path as the
     * other arm and the comparison would prove nothing.
     */
    @SneakyThrows
    @SuppressWarnings("unchecked")
    private void assertConcurrentSearchUsesSeveralSlices(final String index) {
        final Request request = new Request("POST", "/" + index + "/_search");
        request.addParameter("search_pipeline", MIN_MAX_PIPELINE);
        request.addParameter("request_cache", "false");
        request.setJsonEntity(
            XContentFactory.jsonBuilder().map(body(hybrid(match("alpha"), match("beta")), "size", 5, "profile", true)).toString()
        );
        final Map<String, Object> response = XContentHelper.convertToMap(
            XContentType.JSON.xContent(),
            EntityUtils.toString(client().performRequest(request).getEntity()),
            false
        );
        final List<Map<String, Object>> shards = (List<Map<String, Object>>) ((Map<String, Object>) response.get("profile")).get("shards");
        assertFalse("no shard profiles for " + index, shards.isEmpty());
        for (Map<String, Object> shard : shards) {
            final List<Map<String, Object>> searches = (List<Map<String, Object>>) shard.get("searches");
            final List<Map<String, Object>> collectors = (List<Map<String, Object>>) searches.get(0).get("collector");
            final int sliceCount = maxSliceCount(collectors);
            assertTrue(
                String.format(Locale.ROOT, "[%s] shard %s searched %d slice(s): %s", index, shard.get("id"), sliceCount, collectors),
                sliceCount > 1
            );
        }
    }

    /** The largest slice_count in a profiled collector tree; core reports it on the collectors that ran concurrently. */
    @SuppressWarnings("unchecked")
    private static int maxSliceCount(final List<Map<String, Object>> collectors) {
        int max = 0;
        for (Map<String, Object> collector : collectors) {
            if (collector.get("slice_count") instanceof Number sliceCount) {
                max = Math.max(max, sliceCount.intValue());
            }
            if (collector.get("children") instanceof List<?> children) {
                max = Math.max(max, maxSliceCount((List<Map<String, Object>>) children));
            }
        }
        return max;
    }

    /** The response, or the status and root cause reason of a failed request. */
    @SneakyThrows
    private Map<String, Object> search(final String index, final Shape shape) {
        final Request request = new Request("POST", "/" + index + "/_search");
        request.addParameter("search_pipeline", shape.pipeline());
        request.addParameter("request_cache", "false");
        request.setJsonEntity(XContentFactory.jsonBuilder().map(shape.body()).toString());
        try {
            final Response response = client().performRequest(request);
            return XContentHelper.convertToMap(XContentType.JSON.xContent(), EntityUtils.toString(response.getEntity()), false);
        } catch (ResponseException e) {
            final Map<String, Object> error = XContentHelper.convertToMap(
                XContentType.JSON.xContent(),
                EntityUtils.toString(e.getResponse().getEntity()),
                false
            );
            // the innermost cause: a failed search phase wraps the processor's exception
            Map<?, ?> cause = (Map<?, ?>) error.get("error");
            while (Objects.nonNull(cause.get("caused_by"))) {
                cause = (Map<?, ?>) cause.get("caused_by");
            }
            return Map.of("status", e.getResponse().getStatusLine().getStatusCode(), "reason", String.valueOf(cause.get("reason")));
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> hitsOf(final Map<String, Object> response) {
        return (Map<String, Object>) response.get("hits");
    }

    /** Per hit, everything a slice layout could change: id, score, sort values and fields (the collapse value). */
    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> hitSummaries(final Map<String, Object> response) {
        final List<Map<String, Object>> summaries = new ArrayList<>();
        for (Map<String, Object> hit : (List<Map<String, Object>>) hitsOf(response).get("hits")) {
            final Map<String, Object> summary = new LinkedHashMap<>();
            summary.put("_id", hit.get("_id"));
            summary.put("_score", hit.get("_score"));
            summary.put("sort", hit.get("sort"));
            summary.put("fields", hit.get("fields"));
            summaries.add(summary);
        }
        return summaries;
    }

    private static Shape shape(final String name, final String pipeline, final Map<String, Object> body) {
        return new Shape(name, pipeline, body, Expect.HITS);
    }

    private static Map<String, Object> body(final Object query, final Object... keysAndValues) {
        final Map<String, Object> body = new LinkedHashMap<>();
        body.put("query", query);
        for (int i = 0; i < keysAndValues.length; i += 2) {
            body.put((String) keysAndValues[i], keysAndValues[i + 1]);
        }
        return body;
    }

    private static Map<String, Object> hybrid(final Object... queries) {
        return Map.of("hybrid", Map.of("queries", List.of(queries)));
    }

    private static Map<String, Object> paginatedHybrid(final int paginationDepth, final Object... queries) {
        return Map.of("hybrid", Map.of("pagination_depth", paginationDepth, "queries", List.of(queries)));
    }

    private static Map<String, Object> match(final String text) {
        return Map.of("match", Map.of(TEXT_FIELD, text));
    }

    /** Raw vector query; the filter keeps every segment below k, so Lucene searches it exactly and the hits do not depend on slices. */
    private static Map<String, Object> knn() {
        return Map.of(
            "knn",
            Map.of(
                VECTOR_FIELD,
                Map.of("vector", List.of(3.0f, 5.0f, 7.0f), "k", 100, "filter", Map.of("exists", Map.of("field", VECTOR_FIELD)))
            )
        );
    }

    private static Map<String, Object> collapse(final String field) {
        return Map.of("field", field);
    }

    private static Map<String, Object> order(final String field, final String order) {
        return Map.of(field, Map.of("order", order));
    }
}
