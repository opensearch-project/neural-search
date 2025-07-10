/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

import java.util.HashMap;
import java.util.Map;

import org.opensearch.common.ValidationException;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.mapper.SparseMethodContext;

import static org.opensearch.neuralsearch.sparse.common.SparseConstants.SUMMARY_PRUNE_RATIO_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.N_POSTINGS_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.CLUSTER_RATIO_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.ALGO_TRIGGER_DOC_COUNT_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.NAME_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.PARAMETERS_FIELD;

public class SeismicTests extends AbstractSparseTestBase {

    public void testValidateMethod_invalidAlgoTriggerDocCount() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(ALGO_TRIGGER_DOC_COUNT_FIELD, -1);

        Map<String, Object> methodMap = new HashMap<>();
        methodMap.put(NAME_FIELD, "testMethod");
        methodMap.put(PARAMETERS_FIELD, parameters);
        SparseMethodContext context = SparseMethodContext.parse(methodMap);

        ValidationException result = Seismic.INSTANCE.validateMethod(context);

        assertNotNull(result);
        assertTrue(result.validationErrors().contains("algo trigger doc count should be a non-negative integer"));
    }

    public void testValidateMethod_invalidClusterRatio() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(CLUSTER_RATIO_FIELD, 1.0f);

        Map<String, Object> methodMap = new HashMap<>();
        methodMap.put(NAME_FIELD, "testMethod");
        methodMap.put(PARAMETERS_FIELD, parameters);
        SparseMethodContext context = SparseMethodContext.parse(methodMap);

        ValidationException result = Seismic.INSTANCE.validateMethod(context);

        assertNotNull(result);
        assertTrue(result.validationErrors().contains("cluster ratio should be in (0, 1)"));
    }

    public void testValidateMethod_invalidNPostings() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(N_POSTINGS_FIELD, -1);

        Map<String, Object> methodMap = new HashMap<>();
        methodMap.put(NAME_FIELD, "testMethod");
        methodMap.put(PARAMETERS_FIELD, parameters);
        SparseMethodContext context = SparseMethodContext.parse(methodMap);

        ValidationException result = Seismic.INSTANCE.validateMethod(context);

        assertNotNull(result);
        assertTrue(result.validationErrors().contains("n_postings should be a positive integer"));
    }

    public void testValidateMethod_invalidSummaryPruneRatio() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(SUMMARY_PRUNE_RATIO_FIELD, 2.0f);

        Map<String, Object> methodMap = new HashMap<>();
        methodMap.put(NAME_FIELD, "testMethod");
        methodMap.put(PARAMETERS_FIELD, parameters);
        SparseMethodContext context = SparseMethodContext.parse(methodMap);

        ValidationException result = Seismic.INSTANCE.validateMethod(context);

        assertNotNull(result);
        assertTrue(result.validationErrors().contains("summary prune ratio should be in (0, 1]"));
    }

    public void testValidateMethod_multipleInvalidParameters() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(N_POSTINGS_FIELD, 0);
        parameters.put(CLUSTER_RATIO_FIELD, 1.5f);
        parameters.put(ALGO_TRIGGER_DOC_COUNT_FIELD, -1);

        Map<String, Object> methodMap = new HashMap<>();
        methodMap.put(NAME_FIELD, "testMethod");
        methodMap.put(PARAMETERS_FIELD, parameters);
        SparseMethodContext sparseMethodContext = SparseMethodContext.parse(methodMap);

        ValidationException validationException = Seismic.INSTANCE.validateMethod(sparseMethodContext);

        assertNotNull(validationException);
        assertTrue(validationException.validationErrors().contains("n_postings should be a positive integer"));
        assertTrue(validationException.validationErrors().contains("cluster ratio should be in (0, 1)"));
        assertTrue(validationException.validationErrors().contains("algo trigger doc count should be a non-negative integer"));
    }

    public void testValidateMethod_unknownParameter() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("unknown_param", "value");

        Map<String, Object> methodMap = new HashMap<>();
        methodMap.put(NAME_FIELD, "testMethod");
        methodMap.put(PARAMETERS_FIELD, parameters);
        SparseMethodContext context = SparseMethodContext.parse(methodMap);

        ValidationException result = Seismic.INSTANCE.validateMethod(context);

        assertNotNull(result);
        assertTrue(result.validationErrors().contains("Unknown parameter 'unknown_param' found"));
    }

    public void testValidateMethod_validParameters() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(SUMMARY_PRUNE_RATIO_FIELD, 0.5f);
        parameters.put(N_POSTINGS_FIELD, 10);
        parameters.put(CLUSTER_RATIO_FIELD, 0.5f);
        parameters.put(ALGO_TRIGGER_DOC_COUNT_FIELD, 100);

        Map<String, Object> methodMap = new HashMap<>();
        methodMap.put(NAME_FIELD, "testMethod");
        methodMap.put(PARAMETERS_FIELD, parameters);
        SparseMethodContext sparseMethodContext = SparseMethodContext.parse(methodMap);

        ValidationException result = Seismic.INSTANCE.validateMethod(sparseMethodContext);

        assertNull(result);
    }

    public void testValidateMethod_allInvalidParameters() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(SUMMARY_PRUNE_RATIO_FIELD, 0.0f);
        parameters.put(N_POSTINGS_FIELD, 0);
        parameters.put(CLUSTER_RATIO_FIELD, 1.0f);
        parameters.put(ALGO_TRIGGER_DOC_COUNT_FIELD, -1);
        parameters.put("unknown_param", "value");

        Map<String, Object> methodMap = new HashMap<>();
        methodMap.put(NAME_FIELD, "testMethod");
        methodMap.put(PARAMETERS_FIELD, parameters);
        SparseMethodContext sparseMethodContext = SparseMethodContext.parse(methodMap);

        ValidationException result = Seismic.INSTANCE.validateMethod(sparseMethodContext);

        assertNotNull(result);
        assertTrue(result.validationErrors().contains("summary prune ratio should be in (0, 1]"));
        assertTrue(result.validationErrors().contains("n_postings should be a positive integer"));
        assertTrue(result.validationErrors().contains("cluster ratio should be in (0, 1)"));
        assertTrue(result.validationErrors().contains("algo trigger doc count should be a non-negative integer"));
        assertTrue(result.validationErrors().contains("Unknown parameter 'unknown_param' found"));
    }
}
