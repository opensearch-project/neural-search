/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.query.visitor;

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.search.BooleanClause;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;
import org.opensearch.test.OpenSearchTestCase;

public class NeuralSearchQueryVisitorTests extends OpenSearchTestCase {

    public void testAccept_whenNeuralQueryBuilderWithoutModelId_thenSetModelId() {
        String modelId = "bdcvjkcdjvkddcjxdjsc";
        NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder();
        neuralQueryBuilder.fieldName("passage_text");
        neuralQueryBuilder.k(768);

        NeuralSearchQueryVisitor neuralSearchQueryVisitor = new NeuralSearchQueryVisitor(modelId, null);
        neuralSearchQueryVisitor.accept(neuralQueryBuilder);

        assertEquals(modelId, neuralQueryBuilder.modelId());
    }

    public void testAccept_whenNeuralQueryBuilderWithoutFieldModelId_thenSetFieldModelId() {
        Map<String, Object> neuralInfoMap = new HashMap<>();
        neuralInfoMap.put("passage_text", "bdcvjkcdjvkddcjxdjsc");
        NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder();
        neuralQueryBuilder.fieldName("passage_text");
        neuralQueryBuilder.k(768);

        NeuralSearchQueryVisitor neuralSearchQueryVisitor = new NeuralSearchQueryVisitor(null, neuralInfoMap);
        neuralSearchQueryVisitor.accept(neuralQueryBuilder);

        assertEquals("bdcvjkcdjvkddcjxdjsc", neuralQueryBuilder.modelId());
    }

    public void testAccept_whenNullValuesInVisitor_thenFail() {
        NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder();
        NeuralSearchQueryVisitor neuralSearchQueryVisitor = new NeuralSearchQueryVisitor(null, null);

        expectThrows(IllegalArgumentException.class, () -> neuralSearchQueryVisitor.accept(neuralQueryBuilder));
    }

    public void testGetChildVisitor() {
        NeuralSearchQueryVisitor neuralSearchQueryVisitor = new NeuralSearchQueryVisitor(null, null);

        NeuralSearchQueryVisitor subVisitor = (NeuralSearchQueryVisitor) neuralSearchQueryVisitor.getChildVisitor(BooleanClause.Occur.MUST);

        assertEquals(subVisitor, neuralSearchQueryVisitor);

    }
}
