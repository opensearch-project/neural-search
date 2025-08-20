/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm.seismic;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.opensearch.common.ValidationException;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.sparse.mapper.SparseMethodContext;

import static org.opensearch.neuralsearch.sparse.common.SparseConstants.SUMMARY_PRUNE_RATIO_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.N_POSTINGS_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.CLUSTER_RATIO_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.APPROXIMATE_THRESHOLD_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.NAME_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.PARAMETERS_FIELD;

public class SeismicTests extends AbstractSparseTestBase {

    public void testValidateMethod_invalidAlgoTriggerDocCount() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(APPROXIMATE_THRESHOLD_FIELD, -1);

        Map<String, Object> methodMap = new HashMap<>();
        methodMap.put(NAME_FIELD, "testMethod");
        methodMap.put(PARAMETERS_FIELD, parameters);
        SparseMethodContext context = SparseMethodContext.parse(methodMap);

        ValidationException result = Seismic.INSTANCE.validateMethod(context);

        assertNotNull(result);
        String expectedError = String.format(Locale.ROOT, "Parameter [%s] must be a non-Negative integer", APPROXIMATE_THRESHOLD_FIELD);
        assertTrue(result.validationErrors().contains(expectedError));
    }

    public void testValidateMethod_validAlgoTriggerStringNumberDocCount() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(APPROXIMATE_THRESHOLD_FIELD, "12");

        Map<String, Object> methodMap = new HashMap<>();
        methodMap.put(NAME_FIELD, "testMethod");
        methodMap.put(PARAMETERS_FIELD, parameters);
        SparseMethodContext context = SparseMethodContext.parse(methodMap);

        ValidationException result = Seismic.INSTANCE.validateMethod(context);

        assertNull(result);
    }

    public void testValidateMethod_invalidAlgoTriggerStringTextDocCount() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(APPROXIMATE_THRESHOLD_FIELD, "invalid number");

        Map<String, Object> methodMap = new HashMap<>();
        methodMap.put(NAME_FIELD, "testMethod");
        methodMap.put(PARAMETERS_FIELD, parameters);
        SparseMethodContext context = SparseMethodContext.parse(methodMap);

        ValidationException result = Seismic.INSTANCE.validateMethod(context);

        assertNotNull(result);
        String expectedError = String.format(
            Locale.ROOT,
            "Parameter [%s] must be of %s type",
            APPROXIMATE_THRESHOLD_FIELD,
            Integer.class.getName()
        );
        assertTrue(result.validationErrors().contains(expectedError));
    }

    public void testValidateMethod_invalidAlgoTriggerBooleanDocCount() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(APPROXIMATE_THRESHOLD_FIELD, false);

        Map<String, Object> methodMap = new HashMap<>();
        methodMap.put(NAME_FIELD, "testMethod");
        methodMap.put(PARAMETERS_FIELD, parameters);
        SparseMethodContext context = SparseMethodContext.parse(methodMap);

        ValidationException result = Seismic.INSTANCE.validateMethod(context);

        assertNotNull(result);
        String expectedError = String.format(
            Locale.ROOT,
            "Parameter [%s] must be of %s type",
            APPROXIMATE_THRESHOLD_FIELD,
            Integer.class.getName()
        );
        assertTrue(result.validationErrors().contains(expectedError));
    }

    public void testValidateMethod_validSummaryPruneRatioStringNumberDocCount() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(SUMMARY_PRUNE_RATIO_FIELD, "0.5");

        Map<String, Object> methodMap = new HashMap<>();
        methodMap.put(NAME_FIELD, "testMethod");
        methodMap.put(PARAMETERS_FIELD, parameters);
        SparseMethodContext context = SparseMethodContext.parse(methodMap);

        ValidationException result = Seismic.INSTANCE.validateMethod(context);

        assertNull(result);
    }

    public void testValidateMethod_invalidSummaryPruneRatioStringTextDocCount() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(SUMMARY_PRUNE_RATIO_FIELD, "invalid number");

        Map<String, Object> methodMap = new HashMap<>();
        methodMap.put(NAME_FIELD, "testMethod");
        methodMap.put(PARAMETERS_FIELD, parameters);
        SparseMethodContext context = SparseMethodContext.parse(methodMap);

        ValidationException result = Seismic.INSTANCE.validateMethod(context);

        assertNotNull(result);
        String expectedError = String.format(
            Locale.ROOT,
            "Parameter [%s] must be of %s type",
            SUMMARY_PRUNE_RATIO_FIELD,
            Float.class.getName()
        );
        assertTrue(result.validationErrors().contains(expectedError));
    }

    public void testValidateMethod_invalidSummaryPruneRatioBooleanDocCount() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(SUMMARY_PRUNE_RATIO_FIELD, false);

        Map<String, Object> methodMap = new HashMap<>();
        methodMap.put(NAME_FIELD, "testMethod");
        methodMap.put(PARAMETERS_FIELD, parameters);
        SparseMethodContext context = SparseMethodContext.parse(methodMap);

        ValidationException result = Seismic.INSTANCE.validateMethod(context);

        assertNotNull(result);
        String expectedError = String.format(
            Locale.ROOT,
            "Parameter [%s] must be of %s type",
            SUMMARY_PRUNE_RATIO_FIELD,
            Float.class.getName()
        );
        assertTrue(result.validationErrors().contains(expectedError));
    }

    public void testValidateMethod_validPostingFieldStringNumberDocCount() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(N_POSTINGS_FIELD, "4000");

        Map<String, Object> methodMap = new HashMap<>();
        methodMap.put(NAME_FIELD, "testMethod");
        methodMap.put(PARAMETERS_FIELD, parameters);
        SparseMethodContext context = SparseMethodContext.parse(methodMap);

        ValidationException result = Seismic.INSTANCE.validateMethod(context);

        assertNull(result);
    }

    public void testValidateMethod_invalidPostingFieldStringTextDocCount() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(N_POSTINGS_FIELD, "invalid number");

        Map<String, Object> methodMap = new HashMap<>();
        methodMap.put(NAME_FIELD, "testMethod");
        methodMap.put(PARAMETERS_FIELD, parameters);
        SparseMethodContext context = SparseMethodContext.parse(methodMap);

        ValidationException result = Seismic.INSTANCE.validateMethod(context);

        assertNotNull(result);
        String expectedError = String.format(Locale.ROOT, "Parameter [%s] must be of %s type", N_POSTINGS_FIELD, Integer.class.getName());
        assertTrue(result.validationErrors().contains(expectedError));
    }

    public void testValidateMethod_invalidPostingFieldBooleanDocCount() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(N_POSTINGS_FIELD, false);

        Map<String, Object> methodMap = new HashMap<>();
        methodMap.put(NAME_FIELD, "testMethod");
        methodMap.put(PARAMETERS_FIELD, parameters);
        SparseMethodContext context = SparseMethodContext.parse(methodMap);

        ValidationException result = Seismic.INSTANCE.validateMethod(context);

        assertNotNull(result);
        String expectedError = String.format(Locale.ROOT, "Parameter [%s] must be of %s type", N_POSTINGS_FIELD, Integer.class.getName());
        assertTrue(result.validationErrors().contains(expectedError));
    }

    public void testValidateMethod_validClusterRatioStringNumberDocCount() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(CLUSTER_RATIO_FIELD, "0.5");

        Map<String, Object> methodMap = new HashMap<>();
        methodMap.put(NAME_FIELD, "testMethod");
        methodMap.put(PARAMETERS_FIELD, parameters);
        SparseMethodContext context = SparseMethodContext.parse(methodMap);

        ValidationException result = Seismic.INSTANCE.validateMethod(context);

        assertNull(result);
    }

    public void testValidateMethod_invalidClusterRatioStringTextDocCount() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(CLUSTER_RATIO_FIELD, "invalid number");

        Map<String, Object> methodMap = new HashMap<>();
        methodMap.put(NAME_FIELD, "testMethod");
        methodMap.put(PARAMETERS_FIELD, parameters);
        SparseMethodContext context = SparseMethodContext.parse(methodMap);

        ValidationException result = Seismic.INSTANCE.validateMethod(context);

        assertNotNull(result);
        String expectedError = String.format(Locale.ROOT, "Parameter [%s] must be of %s type", CLUSTER_RATIO_FIELD, Float.class.getName());
        assertTrue(result.validationErrors().contains(expectedError));
    }

    public void testValidateMethod_invalidClusterRatioBooleanDocCount() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(CLUSTER_RATIO_FIELD, false);

        Map<String, Object> methodMap = new HashMap<>();
        methodMap.put(NAME_FIELD, "testMethod");
        methodMap.put(PARAMETERS_FIELD, parameters);
        SparseMethodContext context = SparseMethodContext.parse(methodMap);

        ValidationException result = Seismic.INSTANCE.validateMethod(context);

        assertNotNull(result);
        String expectedError = String.format(Locale.ROOT, "Parameter [%s] must be of %s type", CLUSTER_RATIO_FIELD, Float.class.getName());
        assertTrue(result.validationErrors().contains(expectedError));
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
        String expectedError = String.format(Locale.ROOT, "Parameter [%s] must be in (0, 1)", CLUSTER_RATIO_FIELD);
        assertTrue(result.validationErrors().contains(expectedError));
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
        String expectedError = String.format(Locale.ROOT, "Parameter [%s] must be a positive integer", N_POSTINGS_FIELD);
        assertTrue(result.validationErrors().contains(expectedError));
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
        String expectedError = String.format(Locale.ROOT, "Parameter [%s] must be in (0, 1]", SUMMARY_PRUNE_RATIO_FIELD);
        assertTrue(result.validationErrors().contains(expectedError));
    }

    public void testValidateMethod_multipleInvalidParameters() {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(N_POSTINGS_FIELD, 0);
        parameters.put(CLUSTER_RATIO_FIELD, -1.5f);
        parameters.put(APPROXIMATE_THRESHOLD_FIELD, -1);

        Map<String, Object> methodMap = new HashMap<>();
        methodMap.put(NAME_FIELD, "testMethod");
        methodMap.put(PARAMETERS_FIELD, parameters);
        SparseMethodContext sparseMethodContext = SparseMethodContext.parse(methodMap);

        ValidationException validationException = Seismic.INSTANCE.validateMethod(sparseMethodContext);

        assertNotNull(validationException);
        String expectedError1 = String.format(Locale.ROOT, "Parameter [%s] must be a positive integer", N_POSTINGS_FIELD);
        String expectedError2 = String.format(Locale.ROOT, "Parameter [%s] must be in (0, 1)", CLUSTER_RATIO_FIELD);
        String expectedError3 = String.format(Locale.ROOT, "Parameter [%s] must be a non-Negative integer", APPROXIMATE_THRESHOLD_FIELD);
        assertTrue(validationException.validationErrors().contains(expectedError1));
        assertTrue(validationException.validationErrors().contains(expectedError2));
        assertTrue(validationException.validationErrors().contains(expectedError3));
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
        parameters.put(APPROXIMATE_THRESHOLD_FIELD, 100);

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
        parameters.put(APPROXIMATE_THRESHOLD_FIELD, -1);
        parameters.put("unknown_param", "value");

        Map<String, Object> methodMap = new HashMap<>();
        methodMap.put(NAME_FIELD, "testMethod");
        methodMap.put(PARAMETERS_FIELD, parameters);
        SparseMethodContext sparseMethodContext = SparseMethodContext.parse(methodMap);

        ValidationException result = Seismic.INSTANCE.validateMethod(sparseMethodContext);

        assertNotNull(result);
        String expectedError1 = String.format(Locale.ROOT, "Parameter [%s] must be a positive integer", N_POSTINGS_FIELD);
        String expectedError2 = String.format(Locale.ROOT, "Parameter [%s] must be in (0, 1)", CLUSTER_RATIO_FIELD);
        String expectedError3 = String.format(Locale.ROOT, "Parameter [%s] must be a non-Negative integer", APPROXIMATE_THRESHOLD_FIELD);
        String expectedError4 = String.format(Locale.ROOT, "Parameter [%s] must be in (0, 1]", SUMMARY_PRUNE_RATIO_FIELD);
        assertTrue(result.validationErrors().contains(expectedError1));
        assertTrue(result.validationErrors().contains(expectedError2));
        assertTrue(result.validationErrors().contains(expectedError3));
        assertTrue(result.validationErrors().contains(expectedError4));
        assertTrue(result.validationErrors().contains("Unknown parameter 'unknown_param' found"));
    }
}
