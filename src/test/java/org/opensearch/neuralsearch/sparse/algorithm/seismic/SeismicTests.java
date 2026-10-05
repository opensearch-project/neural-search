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
import org.opensearch.neuralsearch.sparse.algorithm.SparseForwardIndex;
import org.opensearch.neuralsearch.sparse.mapper.SparseMethodContext;

import static org.opensearch.neuralsearch.sparse.common.SparseConstants.CLUSTERING_BATCH_SIZE_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.FORWARD_INDEX_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.Seismic.MAX_CLUSTERING_BATCH_SIZE;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.Seismic.MIN_CLUSTERING_BATCH_SIZE;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.SUMMARY_PRUNE_RATIO_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.N_POSTINGS_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.QUANTIZATION_CEILING_INGEST_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.QUANTIZATION_CEILING_SEARCH_FIELD;
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
        String expectedError = String.format(Locale.ROOT, "Parameter [%s] must be a non-negative integer", APPROXIMATE_THRESHOLD_FIELD);
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
        String expectedError3 = String.format(Locale.ROOT, "Parameter [%s] must be a non-negative integer", APPROXIMATE_THRESHOLD_FIELD);
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
        String expectedError3 = String.format(Locale.ROOT, "Parameter [%s] must be a non-negative integer", APPROXIMATE_THRESHOLD_FIELD);
        String expectedError4 = String.format(Locale.ROOT, "Parameter [%s] must be in (0, 1]", SUMMARY_PRUNE_RATIO_FIELD);
        assertTrue(result.validationErrors().contains(expectedError1));
        assertTrue(result.validationErrors().contains(expectedError2));
        assertTrue(result.validationErrors().contains(expectedError3));
        assertTrue(result.validationErrors().contains(expectedError4));
        assertTrue(result.validationErrors().contains("Unknown parameter 'unknown_param' found"));
    }

    // ---- quantization ceilings ----

    public void testValidateMethod_validQuantizationCeilings() {
        ValidationException result = validate(Map.of(QUANTIZATION_CEILING_INGEST_FIELD, 3.0f, QUANTIZATION_CEILING_SEARCH_FIELD, 16.0f));

        assertNull(result);
    }

    public void testValidateMethod_quantizationCeilingIngestMustBePositive() {
        ValidationException result = validate(Map.of(QUANTIZATION_CEILING_INGEST_FIELD, 0.0f));

        assertNotNull(result);
        assertTrue(
            result.validationErrors()
                .contains(String.format(Locale.ROOT, "Parameter [%s] must be a positive float number", QUANTIZATION_CEILING_INGEST_FIELD))
        );
    }

    public void testValidateMethod_quantizationCeilingSearchMustBePositive() {
        ValidationException result = validate(Map.of(QUANTIZATION_CEILING_SEARCH_FIELD, -1.5f));

        assertNotNull(result);
        assertTrue(
            result.validationErrors()
                .contains(String.format(Locale.ROOT, "Parameter [%s] must be a positive float number", QUANTIZATION_CEILING_SEARCH_FIELD))
        );
    }

    public void testValidateMethod_quantizationCeilingIngestMustBeAFloat() {
        ValidationException result = validate(Map.of(QUANTIZATION_CEILING_INGEST_FIELD, "not_a_float"));

        assertNotNull(result);
        assertTrue(
            result.validationErrors()
                .contains(
                    String.format(
                        Locale.ROOT,
                        "Parameter [%s] must be of %s type",
                        QUANTIZATION_CEILING_INGEST_FIELD,
                        Float.class.getName()
                    )
                )
        );
    }

    public void testValidateMethod_quantizationCeilingSearchMustBeAFloat() {
        ValidationException result = validate(Map.of(QUANTIZATION_CEILING_SEARCH_FIELD, "not_a_float"));

        assertNotNull(result);
        assertTrue(
            result.validationErrors()
                .contains(
                    String.format(
                        Locale.ROOT,
                        "Parameter [%s] must be of %s type",
                        QUANTIZATION_CEILING_SEARCH_FIELD,
                        Float.class.getName()
                    )
                )
        );
    }

    // ---- forward index ----

    public void testValidateMethod_validForwardIndex() {
        assertNull(validate(Map.of(FORWARD_INDEX_FIELD, SparseForwardIndex.PER_BLOCK.getName())));
    }

    public void testValidateMethod_invalidForwardIndex() {
        ValidationException result = validate(Map.of(FORWARD_INDEX_FIELD, "not_a_forward_index"));

        assertNotNull(result);
        assertTrue(
            result.validationErrors()
                .contains(
                    String.format(Locale.ROOT, "Parameter [%s] must be one of [%s]", FORWARD_INDEX_FIELD, SparseForwardIndex.validNames())
                )
        );
    }

    // ---- clustering batch size ----

    public void testValidateMethod_validClusteringBatchSize() {
        assertNull(validate(Map.of(CLUSTERING_BATCH_SIZE_FIELD, MAX_CLUSTERING_BATCH_SIZE)));
        assertNull(validate(Map.of(CLUSTERING_BATCH_SIZE_FIELD, MIN_CLUSTERING_BATCH_SIZE)));
        assertNull(validate(Map.of(CLUSTERING_BATCH_SIZE_FIELD, "64")));
    }

    public void testValidateMethod_clusteringBatchSizeBelowRange() {
        assertTrue(validate(Map.of(CLUSTERING_BATCH_SIZE_FIELD, 0)).validationErrors().contains(outOfRangeBatchSizeError()));
    }

    public void testValidateMethod_clusteringBatchSizeAboveRange() {
        assertTrue(
            validate(Map.of(CLUSTERING_BATCH_SIZE_FIELD, MAX_CLUSTERING_BATCH_SIZE + 1)).validationErrors()
                .contains(outOfRangeBatchSizeError())
        );
    }

    public void testValidateMethod_clusteringBatchSizeMustBeAnInteger() {
        String expectedError = String.format(
            Locale.ROOT,
            "Parameter [%s] must be of %s type",
            CLUSTERING_BATCH_SIZE_FIELD,
            Integer.class.getName()
        );

        assertTrue(validate(Map.of(CLUSTERING_BATCH_SIZE_FIELD, "not_an_integer")).validationErrors().contains(expectedError));
        assertTrue(validate(Map.of(CLUSTERING_BATCH_SIZE_FIELD, 1.5f)).validationErrors().contains(expectedError));
    }

    private String outOfRangeBatchSizeError() {
        return String.format(
            Locale.ROOT,
            "Parameter [%s] must be in [%d, %d]",
            CLUSTERING_BATCH_SIZE_FIELD,
            MIN_CLUSTERING_BATCH_SIZE,
            MAX_CLUSTERING_BATCH_SIZE
        );
    }

    private ValidationException validate(Map<String, Object> parameters) {
        Map<String, Object> methodMap = new HashMap<>();
        methodMap.put(NAME_FIELD, "testMethod");
        methodMap.put(PARAMETERS_FIELD, new HashMap<>(parameters));
        return Seismic.INSTANCE.validateMethod(SparseMethodContext.parse(methodMap));
    }
}
