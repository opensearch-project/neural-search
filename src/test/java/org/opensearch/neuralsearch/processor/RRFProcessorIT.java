/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import com.google.common.primitives.Floats;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.junit.Before;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.knn.index.query.KNNQueryBuilder;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.neuralsearch.stats.events.EventStatName;
import org.opensearch.neuralsearch.stats.info.InfoStatName;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.neuralsearch.util.TestUtils.DELTA_FOR_SCORE_ASSERTION;
import static org.opensearch.neuralsearch.util.TestUtils.TEST_SPACE_TYPE;
import static org.opensearch.neuralsearch.util.TestUtils.createRandomVector;

@Log4j2
public class RRFProcessorIT extends BaseNeuralSearchIT {

    private int currentDoc = 1;
    private static final String RRF_INDEX_NAME = "rrf-index";
    private static final String RRF_SEARCH_PIPELINE = "rrf-search-pipeline";
    private static final String TEXT_FIELD_TYPE = "text";
    private static final String VECTOR_FIELD_NAME = "passage_embedding";
    private static final String SEARCH_PIPELINE = "search_pipeline";
    private static final String QUERY_TEXT_1 = "cowboy rodeo bronco";
    private static final float[] vectors = { 0.13520712f, 0.89920187f, 0.6205522f, 0.84914577f, 0.08106315f };
    private static final int RRF_DIMENSION = 5;
    private static int NUM_SHARDS = 2;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        prepareKnnIndex(
            RRF_INDEX_NAME,
            Collections.singletonList(new KNNFieldConfig(VECTOR_FIELD_NAME, RRF_DIMENSION, TEST_SPACE_TYPE)),
            NUM_SHARDS
        );
        addDocuments();
        createDefaultRRFSearchPipeline();
    }

    @SneakyThrows
    public void testRRF_whenValidInput_thenSucceed() {
        enableStats();
        ArrayList<HashMap<String, Object>> matchQueryHitsOfShard0 = getSearchHits(
            new MatchQueryBuilder(TEXT_FIELD_TYPE, QUERY_TEXT_1),
            10,
            0
        );

        ArrayList<HashMap<String, Object>> matchQueryHitsOfShard1 = getSearchHits(
            new MatchQueryBuilder(TEXT_FIELD_TYPE, QUERY_TEXT_1),
            10,
            1
        );

        ArrayList<HashMap<String, Object>> knnQueryHitsOfShard0 = getSearchHits(
            KNNQueryBuilder.builder().fieldName(VECTOR_FIELD_NAME).vector(vectors).k(10).build(),
            10,
            0
        );

        ArrayList<HashMap<String, Object>> knnQueryHitsOfShard1 = getSearchHits(
            KNNQueryBuilder.builder().fieldName(VECTOR_FIELD_NAME).vector(vectors).k(10).build(),
            10,
            1
        );

        Map<String, Float> expectedScoresMap = calculateExpectedScores(
            matchQueryHitsOfShard0,
            matchQueryHitsOfShard1,
            knnQueryHitsOfShard0,
            knnQueryHitsOfShard1
        );

        HybridQueryBuilder hybridQueryBuilder = getHybridQueryBuilder();

        Map<String, Object> results = search(
            RRF_INDEX_NAME,
            hybridQueryBuilder,
            null,
            10,
            Map.of(SEARCH_PIPELINE, RRF_SEARCH_PIPELINE),
            null
        );
        Map<String, Object> hits = (Map<String, Object>) results.get("hits");
        ArrayList<HashMap<String, Object>> hitsList = (ArrayList<HashMap<String, Object>>) hits.get("hits");

        for (HashMap<String, Object> hit : hitsList) {
            String id = hit.get("_id").toString();
            assertEquals(expectedScoresMap.get(id), (Double) hit.get("_score"), DELTA_FOR_SCORE_ASSERTION);
        }

        // Get stats
        String responseBody = executeNeuralStatRequest(new ArrayList<>(), new ArrayList<>());
        Map<String, Object> stats = parseInfoStatsResponse(responseBody);
        Map<String, Object> allNodesStats = parseAggregatedNodeStatsResponse(responseBody);

        // Parse json to get stats
        assertEquals(1, getNestedValue(allNodesStats, EventStatName.RRF_PROCESSOR_EXECUTIONS));
        assertEquals(1, getNestedValue(allNodesStats, EventStatName.COMB_TECHNIQUE_RRF_EXECUTIONS));

        assertEquals(1, getNestedValue(stats, InfoStatName.RRF_PROCESSORS));
        assertEquals(1, getNestedValue(stats, InfoStatName.COMB_TECHNIQUE_RRF_PROCESSORS));

        disableStats();
    }

    private HybridQueryBuilder getHybridQueryBuilder() {
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder(TEXT_FIELD_TYPE, QUERY_TEXT_1);
        KNNQueryBuilder knnQueryBuilder = KNNQueryBuilder.builder().fieldName(VECTOR_FIELD_NAME).vector(vectors).k(10).build();

        HybridQueryBuilder hybridQueryBuilder = new HybridQueryBuilder();
        hybridQueryBuilder.add(matchQueryBuilder);
        hybridQueryBuilder.add(knnQueryBuilder);
        return hybridQueryBuilder;
    }

    @SneakyThrows
    private void addDocuments() {
        addDocument(
            "A West Virginia university women 's basketball team , officials , and a small gathering of fans are in a West Virginia arena ."
        );
        addDocument("A wild animal races across an uncut field with a minimal amount of trees .");
        addDocument("People line the stands which advertise Freemont 's orthopedics , a cowboy rides a light brown bucking bronco .");
        addDocument("A man who is riding a wild horse in the rodeo is very near to falling off .");
        addDocument("A rodeo cowboy , wearing a cowboy hat , is being thrown off of a wild white horse .");
        addDocument("A basketball player dribbles down the court while spectators cheer from the packed gymnasium bleachers.");
        addDocument("A deer leaps gracefully through the dense forest, avoiding fallen branches and thick undergrowth.");
        addDocument("The rodeo announcer calls out scores as a cowgirl successfully ropes a calf in the dusty arena.");
        addDocument("A horse and rider navigate through a series of jumps at the equestrian competition held outdoors.");
        addDocument("A bull rider grips tightly to the rope while the massive black bull spins and kicks in the ring.");
        addDocument("Fans wave foam fingers and banners as the home team scores the winning touchdown in the stadium.");
        addDocument("A fox darts between tall grass stalks in the meadow, hunting for small prey near the creek.");
        addDocument("The bronco rider adjusts his hat and spurs before mounting the restless brown horse in the chute.");
        addDocument("A soccer match unfolds on the green field as players sprint toward the goal while crowds watch.");
        addDocument("Wild geese fly in formation across the open sky above the rural farmland and scattered barns.");
    }

    @SneakyThrows
    private void addDocument(String description) {
        addKnnDoc(
            RRF_INDEX_NAME,
            String.valueOf(currentDoc++),
            List.of(VECTOR_FIELD_NAME),
            Collections.singletonList(Floats.asList(createRandomVector(RRF_DIMENSION)).toArray()),
            List.of(TEXT_FIELD_TYPE),
            List.of(description)
        );
    }

    private ArrayList<HashMap<String, Object>> getHits(Map<String, Object> searchResponse) {
        Map<String, Object> hits = (Map<String, Object>) searchResponse.get("hits");
        ArrayList<HashMap<String, Object>> hitsList = (ArrayList<HashMap<String, Object>>) hits.get("hits");
        return hitsList;
    }

    private ArrayList<HashMap<String, Object>> getSearchHits(QueryBuilder queryBuilder, int resultSize, int shardId) {
        Map<String, Object> searchResponse = search(RRF_INDEX_NAME, queryBuilder, null, resultSize, null, List.of(shardId));
        return getHits(searchResponse);
    }

    private Map<String, Float> calculateExpectedScores(
        ArrayList<HashMap<String, Object>> matchQueryHitsOfShard0,
        ArrayList<HashMap<String, Object>> matchQueryHitsOfShard1,
        ArrayList<HashMap<String, Object>> knnQueryHitsOfShard0,
        ArrayList<HashMap<String, Object>> knnQueryHitsOfShard1
    ) {
        List<HashMap<String, Object>> combinedMatchQueryHits = Stream.concat(
            matchQueryHitsOfShard0.stream(),
            matchQueryHitsOfShard1.stream()
        ).sorted((hit1, hit2) -> {
            Double score1 = (Double) hit1.get("_score");
            Double score2 = (Double) hit2.get("_score");
            return Double.compare(score2, score1);
        }).toList();

        Map<String, Float> matchQueryIdToScoreMap = getIdToRRFScoreMap(combinedMatchQueryHits);

        List<HashMap<String, Object>> combinedKNNQueryHits = Stream.concat(knnQueryHitsOfShard0.stream(), knnQueryHitsOfShard1.stream())
            .sorted((hit1, hit2) -> {
                Double score1 = (Double) hit1.get("_score");
                Double score2 = (Double) hit2.get("_score");
                return Double.compare(score2, score1);
            })
            .toList();

        Map<String, Float> knnQueryIdToScoreMap = getIdToRRFScoreMap(combinedKNNQueryHits);

        Map<String, Float> mergedMap = new HashMap<>(matchQueryIdToScoreMap);
        knnQueryIdToScoreMap.forEach((k, v) -> mergedMap.merge(k, v, Float::sum));

        Map<String, Float> sortedMap = mergedMap.entrySet()
            .stream()
            .sorted(Collections.reverseOrder(Map.Entry.comparingByValue())) // Use comparingByValue() for ascending order
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    Map.Entry::getValue,
                    (e1, e2) -> e1, // Merge function for Collectors.toMap (not strictly needed here but good practice)
                    LinkedHashMap::new // Use LinkedHashMap to preserve insertion order (which is now sorted order)
                )
            );
        return sortedMap;
    }

    private Map<String, Float> getIdToRRFScoreMap(List<HashMap<String, Object>> combinedHits) {
        Map<String, Float> idToScoreMap = new HashMap<>();

        for (int i = 0; i < combinedHits.size(); i++) {
            String id = (String) combinedHits.get(i).get("_id");
            idToScoreMap.put(id, BigDecimal.ONE.divide(BigDecimal.valueOf(60 + i + 1), 10, RoundingMode.HALF_UP).floatValue());
        }
        return idToScoreMap;
    }

}
