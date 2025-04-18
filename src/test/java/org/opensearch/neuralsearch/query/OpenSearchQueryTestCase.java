/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.BooleanClause;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilderVisitor;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AnalyzerScope;
import org.opensearch.index.analysis.IndexAnalyzers;
import org.opensearch.index.analysis.NamedAnalyzer;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.similarity.SimilarityService;
import org.opensearch.indices.IndicesModule;
import org.opensearch.indices.mapper.MapperRegistry;
import org.opensearch.plugins.MapperPlugin;
import org.opensearch.plugins.ScriptPlugin;
import org.opensearch.script.ScriptModule;
import org.opensearch.script.ScriptService;
import org.opensearch.test.OpenSearchTestCase;

public abstract class OpenSearchQueryTestCase extends OpenSearchTestCase {

    protected static final float DELTA_FOR_ASSERTION = 0.001f;

    protected final MapperService createMapperService(Version version, XContentBuilder mapping) throws IOException {
        IndexMetadata meta = IndexMetadata.builder("index")
            .settings(Settings.builder().put("index.version.created", version))
            .numberOfReplicas(0)
            .numberOfShards(1)
            .build();
        IndexSettings indexSettings = new IndexSettings(meta, getIndexSettings());
        MapperRegistry mapperRegistry = new IndicesModule(
            Stream.of().filter(p -> p instanceof MapperPlugin).map(p -> (MapperPlugin) p).collect(toList())
        ).getMapperRegistry();
        ScriptModule scriptModule = new ScriptModule(
            Settings.EMPTY,
            Stream.of().filter(p -> p instanceof ScriptPlugin).map(p -> (ScriptPlugin) p).collect(toList())
        );
        ScriptService scriptService = new ScriptService(getIndexSettings(), scriptModule.engines, scriptModule.contexts);
        SimilarityService similarityService = new SimilarityService(indexSettings, scriptService, emptyMap());
        MapperService mapperService = new MapperService(
            indexSettings,
            createIndexAnalyzers(indexSettings),
            xContentRegistry(),
            similarityService,
            mapperRegistry,
            () -> {
                throw new UnsupportedOperationException();
            },
            () -> true,
            scriptService
        );
        merge(mapperService, mapping);
        return mapperService;
    }

    protected Settings getIndexSettings() {
        return Settings.builder().put("index.version.created", Version.CURRENT).build();
    }

    protected IndexAnalyzers createIndexAnalyzers(IndexSettings indexSettings) {
        return new IndexAnalyzers(
            singletonMap("default", new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer())),
            emptyMap(),
            emptyMap()
        );
    }

    protected final void merge(MapperService mapperService, XContentBuilder mapping) throws IOException {
        mapperService.merge("_doc", new CompressedXContent(BytesReference.bytes(mapping)), MapperService.MergeReason.MAPPING_UPDATE);
    }

    protected final XContentBuilder fieldMapping(CheckedConsumer<XContentBuilder, IOException> buildField) throws IOException {
        return mapping(b -> {
            b.startObject("field");
            buildField.accept(b);
            b.endObject();
        });
    }

    protected final XContentBuilder mapping(CheckedConsumer<XContentBuilder, IOException> buildFields) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startObject("_doc").startObject("properties");
        buildFields.accept(builder);
        return builder.endObject().endObject().endObject();
    }

    protected MapperService createMapperService(XContentBuilder mappings) throws IOException {
        return createMapperService(Version.CURRENT, mappings);
    }

    protected MapperService createMapperService() throws IOException {
        return createMapperService(
            fieldMapping(
                b -> b.field("type", "text")
                    .field("fielddata", true)
                    .startObject("fielddata_frequency_filter")
                    .field("min", 2d)
                    .field("min_segment_size", 1000)
                    .endObject()
            )
        );
    }

    protected static Document getDocument(String fieldName, int docId, String fieldValue, FieldType ft) {
        Document doc = new Document();
        doc.add(new TextField("id", Integer.toString(docId), Field.Store.YES));
        doc.add(new Field(fieldName, fieldValue, ft));
        return doc;
    }

    protected static Weight fakeWeight(Query query) {
        return new Weight(query) {

            @Override
            public Explanation explain(LeafReaderContext context, int doc) {
                return null;
            }

            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext leafReaderContext) {
                return null;
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return false;
            }
        };
    }

    static DocIdSetIterator iterator(final int... docs) {
        return new DocIdSetIterator() {

            int i = -1;

            @Override
            public int nextDoc() {
                if (i + 1 == docs.length) {
                    return NO_MORE_DOCS;
                } else {
                    return docs[++i];
                }
            }

            @Override
            public int docID() {
                return i < 0 ? -1 : i == docs.length ? NO_MORE_DOCS : docs[i];
            }

            @Override
            public long cost() {
                return docs.length;
            }

            @Override
            public int advance(int target) throws IOException {
                return slowAdvance(target);
            }
        };
    }

    protected static Scorer scorer(final int[] docs, List<Float> scores, Weight weight) {
        float[] scoresAsArray = new float[scores.size()];
        int i = 0;
        for (float score : scores) {
            scoresAsArray[i++] = score;
        }
        return scorer(docs, scoresAsArray, weight);
    }

    protected static Scorer scorer(final int[] docs, final float[] scores, Weight weight) {
        final DocIdSetIterator iterator = iterator(docs);
        return new Scorer() {

            int lastScoredDoc = -1;

            public DocIdSetIterator iterator() {
                return iterator;
            }

            @Override
            public int docID() {
                return iterator.docID();
            }

            @Override
            public float score() {
                assertNotEquals("score() called twice on doc " + docID(), lastScoredDoc, docID());
                lastScoredDoc = docID();
                final int idx = Arrays.binarySearch(docs, docID());
                return scores[idx];
            }

            @Override
            public float getMaxScore(int upTo) {
                return Float.MAX_VALUE;
            }
        };
    }

    protected static QueryBuilderVisitor createTestVisitor(List<QueryBuilder> visitedQueries) {
        return new QueryBuilderVisitor() {
            @Override
            public void accept(QueryBuilder qb) {
                visitedQueries.add(qb);
            }

            @Override
            public QueryBuilderVisitor getChildVisitor(BooleanClause.Occur occur) {
                return this;
            }
        };
    }
}
