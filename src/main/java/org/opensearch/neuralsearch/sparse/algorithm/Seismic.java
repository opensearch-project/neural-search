/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.math.NumberUtils;
import org.opensearch.common.ValidationException;
import org.opensearch.neuralsearch.sparse.mapper.SparseMethodContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.opensearch.neuralsearch.sparse.common.SparseConstants.APPROXIMATE_THRESHOLD_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.SUMMARY_PRUNE_RATIO_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.CLUSTER_RATIO_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.N_POSTINGS_FIELD;

/**
 * SEISMIC (Sparse Efficient Index Search with Machine Intelligence and Clustering) algorithm implementation.
 *
 * <p>This class implements parameter validation for the SEISMIC sparse algorithm,
 * which is designed for efficient neural search operations. It validates configuration
 * parameters including summary pruning ratios, posting limits, cluster ratios, and
 * approximation thresholds.
 *
 * <p>The SEISMIC algorithm optimizes sparse vector search through intelligent
 * clustering and approximation techniques, providing improved search performance
 * while maintaining result quality.
 *
 * @see SparseAlgorithm
 * @see SparseMethodContext
 */
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class Seismic implements SparseAlgorithm {
    public static final Seismic INSTANCE;

    static {
        INSTANCE = new Seismic();
    }

    /**
     * Validates SEISMIC algorithm parameters.
     *
     * <p>Validates the following parameters:
     * <ul>
     *   <li>summary_prune_ratio: Must be in (0, 1]</li>
     *   <li>n_postings: Must be a positive integer</li>
     *   <li>cluster_ratio: Must be in (0, 1)</li>
     *   <li>approximate_threshold: Must be a non-negative integer</li>
     * </ul>
     *
     * @param sparseMethodContext the method context containing parameters to validate
     * @return ValidationException containing error messages, or null if validation passes
     */
    @Override
    public ValidationException validateMethod(SparseMethodContext sparseMethodContext) {
        String algoName = sparseMethodContext.getMethodComponentContext().getName();
        ValidationException validationException = null;
        final List<String> errorMessages = new ArrayList<>();
        Map<String, Object> parameters = new HashMap<>(sparseMethodContext.getMethodComponentContext().getParameters());
        if (parameters.containsKey(SUMMARY_PRUNE_RATIO_FIELD)) {
            try {
                String fieldValueString = parameters.get(SUMMARY_PRUNE_RATIO_FIELD).toString();
                float summaryPruneRatio = NumberUtils.createFloat(fieldValueString);
                if (summaryPruneRatio <= 0 || summaryPruneRatio > 1) {
                    errorMessages.add(String.format(Locale.ROOT, "Parameter [%s] must be in (0, 1]", SUMMARY_PRUNE_RATIO_FIELD));
                }
            } catch (Exception e) {
                errorMessages.add(
                    String.format(Locale.ROOT, "Parameter [%s] must be of %s type", SUMMARY_PRUNE_RATIO_FIELD, Float.class.getName())
                );
            }
            parameters.remove(SUMMARY_PRUNE_RATIO_FIELD);
        }
        if (parameters.containsKey(N_POSTINGS_FIELD)) {
            try {
                String fieldValueString = parameters.get(N_POSTINGS_FIELD).toString();
                int nPostings = NumberUtils.createInteger(fieldValueString);
                if (nPostings <= 0) {
                    errorMessages.add(String.format(Locale.ROOT, "Parameter [%s] must be a positive integer", N_POSTINGS_FIELD));
                }
            } catch (Exception e) {
                errorMessages.add(
                    String.format(Locale.ROOT, "Parameter [%s] must be of %s type", N_POSTINGS_FIELD, Integer.class.getName())
                );
            }
            parameters.remove(N_POSTINGS_FIELD);
        }
        if (parameters.containsKey(CLUSTER_RATIO_FIELD)) {
            try {
                String fieldValueString = parameters.get(CLUSTER_RATIO_FIELD).toString();
                float clusterRatio = NumberUtils.createFloat(fieldValueString);
                if (clusterRatio <= 0 || clusterRatio >= 1) {
                    errorMessages.add(String.format(Locale.ROOT, "Parameter [%s] must be in (0, 1)", CLUSTER_RATIO_FIELD));
                }
            } catch (Exception e) {
                errorMessages.add(
                    String.format(Locale.ROOT, "Parameter [%s] must be of %s type", CLUSTER_RATIO_FIELD, Float.class.getName())
                );
            }
            parameters.remove(CLUSTER_RATIO_FIELD);
        }
        if (parameters.containsKey(APPROXIMATE_THRESHOLD_FIELD)) {
            try {
                String fieldValueString = parameters.get(APPROXIMATE_THRESHOLD_FIELD).toString();
                int algoTriggerThreshold = NumberUtils.createInteger(fieldValueString);
                if (algoTriggerThreshold < 0) {
                    errorMessages.add(
                        String.format(Locale.ROOT, "Parameter [%s] must be a non-Negative integer", APPROXIMATE_THRESHOLD_FIELD)
                    );
                }
            } catch (Exception e) {
                errorMessages.add(
                    String.format(Locale.ROOT, "Parameter [%s] must be of %s type", APPROXIMATE_THRESHOLD_FIELD, Integer.class.getName())
                );
            }
            parameters.remove(APPROXIMATE_THRESHOLD_FIELD);
        }
        for (String key : parameters.keySet()) {
            errorMessages.add(String.format(Locale.ROOT, "Unknown parameter '%s' found", key));
        }
        if (!errorMessages.isEmpty()) {
            validationException = new ValidationException();
            validationException.addValidationErrors(errorMessages);
        }
        return validationException;
    }
}
