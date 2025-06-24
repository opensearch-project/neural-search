/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import org.opensearch.common.ValidationException;
import org.opensearch.neuralsearch.sparse.mapper.SparseMethodContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.opensearch.neuralsearch.sparse.common.SparseConstants.ALGO_TRIGGER_DOC_COUNT_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.SUMMARY_PRUNE_RATIO_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.CLUSTER_RATIO_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.N_POSTINGS_FIELD;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class Seismic implements SparseAlgorithm {
    public static final Seismic INSTANCE;

    static {
        INSTANCE = new Seismic();
    }

    @Override
    public ValidationException validateMethod(SparseMethodContext sparseMethodContext) {
        String algoName = sparseMethodContext.getMethodComponentContext().getName();
        ValidationException validationException = null;
        final List<String> errorMessages = new ArrayList<>();
        Map<String, Object> parameters = new HashMap<>(sparseMethodContext.getMethodComponentContext().getParameters());
        if (parameters.containsKey(SUMMARY_PRUNE_RATIO_FIELD)) {
            float summaryPruneRatio = ((Number) parameters.get(SUMMARY_PRUNE_RATIO_FIELD)).floatValue();
            if (summaryPruneRatio <= 0 || summaryPruneRatio > 1) {
                errorMessages.add("summary prune ratio should be in (0, 1]");
            }
            parameters.remove(SUMMARY_PRUNE_RATIO_FIELD);
        }
        if (parameters.containsKey(N_POSTINGS_FIELD)) {
            Integer nPostings = (Integer) parameters.get(N_POSTINGS_FIELD);
            if (nPostings <= 0) {
                errorMessages.add("n_postings should be a positive integer");
            }
            parameters.remove(N_POSTINGS_FIELD);
        }
        if (parameters.containsKey(CLUSTER_RATIO_FIELD)) {
            float clusterRatio = ((Number) parameters.get(CLUSTER_RATIO_FIELD)).floatValue();
            if (clusterRatio <= 0 || clusterRatio >= 1) {
                errorMessages.add("cluster ratio should be in (0, 1)");
            }
            parameters.remove(CLUSTER_RATIO_FIELD);
        }
        if (parameters.containsKey(ALGO_TRIGGER_DOC_COUNT_FIELD)) {
            Integer algoTriggerThreshold = (Integer) parameters.get(ALGO_TRIGGER_DOC_COUNT_FIELD);
            if (algoTriggerThreshold < 0) {
                errorMessages.add("algo trigger doc count should be a non-negative integer");
            }
            parameters.remove(ALGO_TRIGGER_DOC_COUNT_FIELD);
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
