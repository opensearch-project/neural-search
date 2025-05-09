/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query.visitor;

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.search.BooleanClause;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder;
import org.opensearch.neuralsearch.util.NeuralSearchClusterTestUtils;
import org.opensearch.test.OpenSearchTestCase;

public class NeuralSearchQueryVisitorTests extends OpenSearchTestCase {

    public void testAccept_whenNeuralQueryBuilderWithoutModelId_thenSetModelId() {
        NeuralSearchClusterTestUtils.setUpClusterService();
        String modelId = "bdcvjkcdjvkddcjxdjsc";
        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName("passage_text")
            .queryText("query_text")
            .k(768)
            .build();

        NeuralSearchQueryVisitor neuralSearchQueryVisitor = new NeuralSearchQueryVisitor(modelId, null);
        neuralSearchQueryVisitor.accept(neuralQueryBuilder);

        assertEquals(modelId, neuralQueryBuilder.modelId());
    }

    public void testAccept_whenNeuralQueryBuilderWithoutFieldModelId_thenSetFieldModelId() {
        NeuralSearchClusterTestUtils.setUpClusterService();
        Map<String, Object> neuralInfoMap = new HashMap<>();
        neuralInfoMap.put("passage_text", "bdcvjkcdjvkddcjxdjsc");
        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder()
            .fieldName("passage_text")
            .queryText("query_text")
            .k(768)
            .build();

        NeuralSearchQueryVisitor neuralSearchQueryVisitor = new NeuralSearchQueryVisitor(null, neuralInfoMap);
        neuralSearchQueryVisitor.accept(neuralQueryBuilder);

        assertEquals("bdcvjkcdjvkddcjxdjsc", neuralQueryBuilder.modelId());
    }

    public void testAccept_whenNeuralSparseQueryBuilderWithModelId_theDoNothing() {
        String modelId1 = "bdcvjkcdjvkddcjxdjsc";
        String modelId2 = "45dfsnfoiqwrjcjxdjsc";
        NeuralSparseQueryBuilder neuralSparseQueryBuilder = new NeuralSparseQueryBuilder();
        neuralSparseQueryBuilder.fieldName("passage_text");
        neuralSparseQueryBuilder.modelId(modelId1);

        NeuralSearchQueryVisitor neuralSearchQueryVisitor = new NeuralSearchQueryVisitor(modelId2, null);
        neuralSearchQueryVisitor.accept(neuralSparseQueryBuilder);

        assertEquals(modelId1, neuralSparseQueryBuilder.modelId());
    }

    public void testAccept_whenNeuralSparseQueryBuilderWithoutModelId_thenSetModelId() {
        String modelId = "bdcvjkcdjvkddcjxdjsc";
        NeuralSparseQueryBuilder neuralSparseQueryBuilder = new NeuralSparseQueryBuilder();
        neuralSparseQueryBuilder.fieldName("passage_text");

        NeuralSearchQueryVisitor neuralSearchQueryVisitor = new NeuralSearchQueryVisitor(modelId, null);
        neuralSearchQueryVisitor.accept(neuralSparseQueryBuilder);

        assertEquals(modelId, neuralSparseQueryBuilder.modelId());
    }

    public void testAccept_whenNeuralSparseQueryBuilderWithoutFieldModelId_thenSetFieldModelId() {
        Map<String, Object> neuralInfoMap = new HashMap<>();
        neuralInfoMap.put("passage_text", "bdcvjkcdjvkddcjxdjsc");
        NeuralSparseQueryBuilder neuralSparseQueryBuilder = new NeuralSparseQueryBuilder();
        neuralSparseQueryBuilder.fieldName("passage_text");

        NeuralSearchQueryVisitor neuralSearchQueryVisitor = new NeuralSearchQueryVisitor(null, neuralInfoMap);
        neuralSearchQueryVisitor.accept(neuralSparseQueryBuilder);

        assertEquals("bdcvjkcdjvkddcjxdjsc", neuralSparseQueryBuilder.modelId());
    }

    public void testAccept_whenNullValuesInVisitor_thenFail() {
        NeuralSearchClusterTestUtils.setUpClusterService();
        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.builder().fieldName("passage_text").queryText("query_text").build();
        NeuralSparseQueryBuilder neuralSparseQueryBuilder = new NeuralSparseQueryBuilder();
        NeuralSearchQueryVisitor neuralSearchQueryVisitor = new NeuralSearchQueryVisitor(null, null);

        expectThrows(IllegalArgumentException.class, () -> neuralSearchQueryVisitor.accept(neuralQueryBuilder));
        expectThrows(IllegalArgumentException.class, () -> neuralSearchQueryVisitor.accept(neuralSparseQueryBuilder));
    }

    public void testGetChildVisitor() {
        NeuralSearchQueryVisitor neuralSearchQueryVisitor = new NeuralSearchQueryVisitor(null, null);

        NeuralSearchQueryVisitor subVisitor = (NeuralSearchQueryVisitor) neuralSearchQueryVisitor.getChildVisitor(BooleanClause.Occur.MUST);

        assertEquals(subVisitor, neuralSearchQueryVisitor);

    }
}
