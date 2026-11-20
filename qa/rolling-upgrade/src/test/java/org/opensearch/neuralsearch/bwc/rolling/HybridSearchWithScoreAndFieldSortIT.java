/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.bwc.rolling;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.opensearch.Version;
import org.opensearch.client.Request;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.search.sort.SortOrder;

import static org.hamcrest.Matchers.containsString;
import static org.opensearch.neuralsearch.util.TestUtils.DEFAULT_COMBINATION_METHOD;
import static org.opensearch.neuralsearch.util.TestUtils.DEFAULT_NORMALIZATION_METHOD;
import static org.opensearch.neuralsearch.util.TestUtils.NODES_BWC_CLUSTER;
import static org.opensearch.neuralsearch.util.TestUtils.PARAM_NAME_WEIGHTS;
import static org.opensearch.neuralsearch.util.TestUtils.TEXT_EMBEDDING_PROCESSOR;
import static org.opensearch.neuralsearch.util.TestUtils.getModelId;

/**
 * The rolling-upgrade contract of {@code MINIMAL_SUPPORTED_VERSION_SCORE_AND_FIELD_SORT_IN_HYBRID_QUERY}: a hybrid query
 * sorted by {@code [_score desc, <field>]} without {@code collapse} is refused until every node in the cluster can serve
 * it, and accepted once they all can.
 *
 * <p>The gate is what makes that safe. A node from before the feature neither orders such a page by the combined score nor
 * applies its {@code search_after} cursor after combination, while a node from the feature collects the whole window for the
 * cursor, so a mixed cluster would page wrong whichever node coordinated. The gate reads the cluster-wide minimum version,
 * so the refusal holds through both mixed rounds and lifts only after the last node is upgraded.
 *
 * <p>This is the case that catches the gate naming the wrong release: with the constant one release too low, the mixed
 * rounds below would accept the shape instead of refusing it.
 */
public class HybridSearchWithScoreAndFieldSortIT extends AbstractRollingUpgradeTestCase {

    private static final String PIPELINE_NAME = "nlp-hybrid-score-field-sort-pipeline";
    private static final String SEARCH_PIPELINE_NAME = "nlp-hybrid-score-field-sort-search-pipeline";
    private static final String TEST_FIELD = "passage_text";
    private static final String SORT_FIELD = "stock";
    private static final String VECTOR_EMBEDDING_FIELD = "passage_embedding";
    // the index is rewritten to a single shard, so routing only keeps the ingest path identical to the sibling ITs
    private static final String ROUTING_KEY = "score-field-sort-routing";
    private static final String QUERY = "Hi world";
    private static final int QUERY_SIZE = 10;
    private static final int PAGINATION_DEPTH = 20;
    private static final List<String> TEXTS = List.of("Hello world", "Hi planet", "Hi earth", "Hi amazon", "Hi mars");
    // the 400 a node that cannot serve the shape answers with, before the feature and while the gate is closed
    private static final String REFUSAL = "_score sort criteria cannot be applied with any other criteria";

    private static String modelId = "";

    public void testHybridSearchWithScoreAndFieldSort_E2EFlow() throws Exception {
        waitForClusterHealthGreen(NODES_BWC_CLUSTER);
        switch (getClusterType()) {
            case OLD:
                modelId = uploadTextEmbeddingModel();
                createPipelineProcessor(modelId, PIPELINE_NAME);
                createIndexWithConfiguration(getIndexNameForTest(), indexConfigurationWithACopyOnEveryNode(), PIPELINE_NAME);
                for (int docId = 0; docId < TEXTS.size(); docId++) {
                    addDocumentWithSortField(getIndexNameForTest(), String.valueOf(docId), TEXTS.get(docId), (docId + 1) * 10);
                }
                createSearchPipeline(
                    SEARCH_PIPELINE_NAME,
                    DEFAULT_NORMALIZATION_METHOD,
                    DEFAULT_COMBINATION_METHOD,
                    Map.of(PARAM_NAME_WEIGHTS, Arrays.toString(new float[] { 0.3f, 0.7f }))
                );
                // the baseline, not the gate: every node predates the feature, so every node refuses by its own missing code
                assertScoreAndFieldSortIsRefused(anyNode());
                break;
            case MIXED:
                modelId = getModelId(getIngestionPipeline(PIPELINE_NAME), TEXT_EMBEDDING_PROCESSOR);
                loadAndWaitForModelToBeReady(modelId);
                // the case that catches the gate naming the wrong release. Served from a node that HAS the feature, so its own
                // code would answer the request; only the gate, reading a cluster minimum that is still the old version, can
                // refuse it. With the constant one release too low this is the assertion that fails.
                assertScoreAndFieldSortIsRefused(highestVersionNode());
                break;
            case UPGRADED:
                try {
                    modelId = getModelId(getIngestionPipeline(PIPELINE_NAME), TEXT_EMBEDDING_PROCESSOR);
                    loadAndWaitForModelToBeReady(modelId);
                    // every node can serve the shape now, so the gate is open
                    assertScoreAndFieldSortIsServed(highestVersionNode());
                } finally {
                    wipeOfTestResources(getIndexNameForTest(), PIPELINE_NAME, modelId, SEARCH_PIPELINE_NAME);
                }
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + getClusterType());
        }
    }

    /** The shape is refused with the same 400 a node without the feature answers with, and no results are returned. */
    private void assertScoreAndFieldSortIsRefused(final String nodeId) throws Exception {
        ResponseException refused = expectThrows(ResponseException.class, () -> searchWithScoreAndFieldSort(null, nodeId));
        Response response = refused.getResponse();
        String body = EntityUtils.toString(response.getEntity());
        assertEquals(
            "a cluster where any node cannot serve [_score desc, <field>] without collapse must refuse it: " + body,
            RestStatus.BAD_REQUEST.getStatus(),
            response.getStatusLine().getStatusCode()
        );
        assertThat("the refusal must be the one a node without the feature gives: " + body, body, containsString(REFUSAL));
    }

    /**
     * The page comes back ordered by the combined score, and its {@code search_after} cursor pages the same order.
     *
     * <p>Asserted on the sort field, not on the {@code _score} component of the cursor. An earlier revision compared the
     * whole cursor and failed in CI with the same document's combined score reported as {@code 0.19918} on one page and
     * {@code 0.19917999} on the next. Why the two differ is not established here — the candidate set, the approximate
     * neural leg and which shard copy answered are all candidates — so the assertion is built not to depend on it. The
     * field identifies the documents, which is what the order has to be checked against; the score is only required to be
     * non-increasing, which is the property the sort actually promises.
     */
    private void assertScoreAndFieldSortIsServed(final String nodeId) {
        Map<String, Object> firstPage = searchWithScoreAndFieldSort(null, nodeId);
        assertNotNull(firstPage);
        assertEquals(TEXTS.size(), getHitCount(firstPage));
        List<List<Object>> cursors = sortValues(firstPage);
        // _score first, then the field that breaks ties between equal combined scores
        assertEquals(2, cursors.get(0).size());
        assertScoresAreNonIncreasing(cursors);

        // paging after the first hit's cursor drops exactly that hit and keeps the rest in the same order
        Map<String, Object> secondPage = searchWithScoreAndFieldSort(cursors.get(0), nodeId);
        assertNotNull(secondPage);
        assertEquals(TEXTS.size() - 1, getHitCount(secondPage));
        List<List<Object>> pagedCursors = sortValues(secondPage);
        assertEquals(sortFieldsOf(cursors.subList(1, cursors.size())), sortFieldsOf(pagedCursors));
        assertScoresAreNonIncreasing(pagedCursors);
    }

    /** The sort field of each cursor: the stable half, and the one that says which documents came back in which order. */
    private static List<Integer> sortFieldsOf(final List<List<Object>> cursors) {
        List<Integer> fields = new ArrayList<>();
        for (List<Object> cursor : cursors) {
            fields.add(((Number) cursor.get(1)).intValue());
        }
        return fields;
    }

    /** {@code _score} is the primary descending key, so it may repeat across a page but must never climb. */
    private static void assertScoresAreNonIncreasing(final List<List<Object>> cursors) {
        for (int hit = 1; hit < cursors.size(); hit++) {
            double previous = ((Number) cursors.get(hit - 1).get(0)).doubleValue();
            double current = ((Number) cursors.get(hit).get(0)).doubleValue();
            assertTrue("_score must not increase down the page, but went " + previous + " -> " + current, current <= previous);
        }
    }

    private Map<String, Object> searchWithScoreAndFieldSort(final List<Object> searchAfter, final String nodeId) {
        List<SortBuilder<?>> sort = new ArrayList<>();
        // _score has to be the primary, descending key; the field only breaks ties below it
        sort.add(SortBuilders.scoreSort().order(SortOrder.DESC));
        sort.add(SortBuilders.fieldSort(SORT_FIELD).order(SortOrder.DESC));
        return search(
            getIndexNameForTest(),
            getQueryBuilder(modelId),
            null,
            QUERY_SIZE,
            // the gate is applied by the node that serves the shard, so which node answers decides what the assertion proves
            Map.of("search_pipeline", SEARCH_PIPELINE_NAME, "preference", "_only_nodes:" + nodeId),
            null,
            null,
            sort,
            false,
            searchAfter,
            0,
            null
        );
    }

    @SuppressWarnings("unchecked")
    private List<List<Object>> sortValues(final Map<String, Object> searchResponseAsMap) {
        Map<String, Object> hitsMap = (Map<String, Object>) searchResponseAsMap.get("hits");
        List<Map<String, Object>> hitsList = (List<Map<String, Object>>) hitsMap.get("hits");
        List<List<Object>> cursors = new ArrayList<>();
        for (Map<String, Object> hit : hitsList) {
            List<Object> cursor = (List<Object>) hit.get("sort");
            assertNotNull(cursor);
            cursors.add(cursor);
        }
        return cursors;
    }

    /**
     * The shared index configuration, with a copy of the shard on every node of the upgrade cluster.
     *
     * <p>The gate is applied by the node that serves the shard, not by the coordinator, so the assertion is only about the
     * gate if the serving node can be chosen. The shared configuration's three shards with one replica leave only two of
     * the three nodes holding a copy of any given shard, and which copy answers is adaptive replica selection's choice, so
     * a search can be served entirely by old nodes in every round - and an old node refuses by its own missing code,
     * never reaching the gate. One shard with a copy on every node lets {@code preference=_only_nodes:<id>} pick any node,
     * and that choice then decides which code answers.
     */
    private String indexConfigurationWithACopyOnEveryNode() throws Exception {
        String shared = Files.readString(Path.of(classLoader.getResource("processor/IndexMappings.json").toURI()));
        String everyNode = shared.replace("\"number_of_shards\": 3", "\"number_of_shards\": 1")
            .replace("\"number_of_replicas\": 1", "\"number_of_replicas\": " + (Integer.parseInt(NODES_BWC_CLUSTER) - 1));
        assertThat(
            "the shared index configuration no longer declares three shards with one replica, so this rewrite is stale",
            everyNode,
            containsString("\"number_of_replicas\": " + (Integer.parseInt(NODES_BWC_CLUSTER) - 1))
        );
        assertThat(everyNode, containsString("\"number_of_shards\": 1"));
        return everyNode;
    }

    /** A node already on the highest version present, which is an upgraded one in any round that has upgraded a node. */
    private String highestVersionNode() throws Exception {
        Map<String, String> versionByNode = nodeVersions();
        String highest = versionByNode.values().stream().max(Comparator.comparing(Version::fromString)).orElseThrow();
        return versionByNode.entrySet()
            .stream()
            .filter(node -> node.getValue().equals(highest))
            .map(Map.Entry::getKey)
            .findFirst()
            .orElseThrow();
    }

    /** Any node, for a round where every node is the same version and the choice carries no meaning. */
    private String anyNode() throws Exception {
        return nodeVersions().keySet().iterator().next();
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> nodeVersions() throws Exception {
        Request request = new Request("GET", "/_nodes/_all/version");
        Map<String, Object> response = entityAsMap(client().performRequest(request));
        Map<String, Object> nodes = (Map<String, Object>) response.get("nodes");
        Map<String, String> versionByNode = new LinkedHashMap<>();
        nodes.forEach((nodeId, node) -> versionByNode.put(nodeId, (String) ((Map<String, Object>) node).get("version")));
        assertEquals(
            "the upgrade cluster must have every node reporting a version",
            Integer.parseInt(NODES_BWC_CLUSTER),
            versionByNode.size()
        );
        return versionByNode;
    }

    private void addDocumentWithSortField(final String index, final String docId, final String text, final int sortFieldValue)
        throws IOException {
        Request request = new Request("PUT", "/" + index + "/_doc/" + docId + "?refresh=true&routing=" + ROUTING_KEY);
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        builder.field(TEST_FIELD, text);
        builder.field(SORT_FIELD, sortFieldValue);
        builder.endObject();
        request.setJsonEntity(builder.toString());
        client().performRequest(request);
    }

    private HybridQueryBuilder getQueryBuilder(final String modelId) {
        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName(VECTOR_EMBEDDING_FIELD)
            .modelId(modelId)
            .queryText(QUERY)
            // k above the document count so every document is a candidate of the neural sub-query and the page is deterministic
            .k(100)
            .build();

        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
        hybridQueryBuilder.add(new MatchQueryBuilder(TEST_FIELD, QUERY));
        hybridQueryBuilder.add(neuralQueryBuilder);
        // the shape requires a depth, which has been accepted since 2.19, so every cluster in this upgrade can parse it.
        // Setting it leaves the version gate as the only reason any of them could refuse the request.
        hybridQueryBuilder.paginationDepth(PAGINATION_DEPTH);
        return hybridQueryBuilder;
    }
}
