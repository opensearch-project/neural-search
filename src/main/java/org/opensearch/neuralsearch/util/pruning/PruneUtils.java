/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.util.pruning;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;

/**
 * Utility class providing methods for pruning sparse vectors using different strategies.
 * Pruning helps reduce the dimensionality of sparse vectors by removing less significant elements
 * based on various criteria.
 */
public class PruneUtils {
    public static final String PRUNE_TYPE_FIELD = "prune_type";
    public static final String PRUNE_RATIO_FIELD = "prune_ratio";

    /**
     * Prunes a sparse vector by keeping only the top K elements with the highest values.
     *
     * @param sparseVector The input sparse vector as a map of string keys to float values
     * @param k The number of top elements to keep
     * @return A new map containing only the top K elements
     */
    private static Map<String, Float> pruningByTopK(Map<String, Float> sparseVector, int k) {
        PriorityQueue<Map.Entry<String, Float>> pq = new PriorityQueue<>((a, b) -> Float.compare(a.getValue(), b.getValue()));

        for (Map.Entry<String, Float> entry : sparseVector.entrySet()) {
            if (pq.size() < k) {
                pq.offer(entry);
            } else if (entry.getValue() > pq.peek().getValue()) {
                pq.poll();
                pq.offer(entry);
            }
        }

        Map<String, Float> result = new HashMap<>();
        while (!pq.isEmpty()) {
            Map.Entry<String, Float> entry = pq.poll();
            result.put(entry.getKey(), entry.getValue());
        }

        return result;
    }

    /**
     * Prunes a sparse vector by keeping only elements whose values are within a certain ratio
     * of the maximum value in the vector.
     *
     * @param sparseVector The input sparse vector as a map of string keys to float values
     * @param ratio The minimum ratio relative to the maximum value for elements to be kept
     * @return A new map containing only elements meeting the ratio threshold
     */
    private static Map<String, Float> pruningByMaxRatio(Map<String, Float> sparseVector, float ratio) {
        float maxValue = sparseVector.values().stream().max(Float::compareTo).orElse(0f);

        Map<String, Float> result = new HashMap<>();
        for (Map.Entry<String, Float> entry : sparseVector.entrySet()) {
            float currentRatio = entry.getValue() / maxValue;

            if (currentRatio >= ratio) {
                result.put(entry.getKey(), entry.getValue());
            }
        }

        return result;
    }

    /**
     * Prunes a sparse vector by removing elements with values below a certain threshold.
     *
     * @param sparseVector The input sparse vector as a map of string keys to float values
     * @param thresh The minimum absolute value for elements to be kept
     * @return A new map containing only elements meeting the threshold
     */
    private static Map<String, Float> pruningByValue(Map<String, Float> sparseVector, float thresh) {
        Map<String, Float> result = new HashMap<>(sparseVector);
        for (Map.Entry<String, Float> entry : sparseVector.entrySet()) {
            if (entry.getValue() < thresh) {
                result.remove(entry.getKey());
            }
        }

        return result;
    }

    /**
     * Prunes a sparse vector by keeping only elements whose cumulative sum of values
     * is within a certain ratio of the total sum.
     *
     * @param sparseVector The input sparse vector as a map of string keys to float values
     * @param alpha The minimum ratio relative to the total sum for elements to be kept
     * @return A new map containing only elements meeting the ratio threshold
     */
    private static Map<String, Float> pruningByAlphaMass(Map<String, Float> sparseVector, float alpha) {
        List<Map.Entry<String, Float>> sortedEntries = new ArrayList<>(sparseVector.entrySet());
        sortedEntries.sort(Map.Entry.comparingByValue(Comparator.reverseOrder()));

        float sum = (float) sparseVector.values().stream().mapToDouble(Float::doubleValue).sum();
        float topSum = 0f;

        Map<String, Float> result = new HashMap<>();
        for (Map.Entry<String, Float> entry : sortedEntries) {
            float value = entry.getValue();
            topSum += value;
            result.put(entry.getKey(), value);

            if (topSum / sum >= alpha) {
                break;
            }
        }

        return result;
    }

    /**
     * Prunes a sparse vector using the specified pruning type and ratio.
     *
     * @param pruningType The type of pruning strategy to use
     * @param pruneRatio The ratio or threshold for pruning
     * @param sparseVector The input sparse vector as a map of string keys to float values
     * @return A new map containing the pruned sparse vector
     */
    public static Map<String, Float> pruningSparseVector(PruningType pruningType, float pruneRatio, Map<String, Float> sparseVector) {
        if (Objects.isNull(pruningType) || Objects.isNull(pruneRatio)) throw new IllegalArgumentException(
            "Prune type and prune ratio must be provided"
        );

        for (Map.Entry<String, Float> entry : sparseVector.entrySet()) {
            if (entry.getValue() <= 0) {
                throw new IllegalArgumentException("Pruned values must be positive");
            }
        }

        switch (pruningType) {
            case TOP_K:
                return pruningByTopK(sparseVector, (int) pruneRatio);
            case ALPHA_MASS:
                return pruningByAlphaMass(sparseVector, pruneRatio);
            case MAX_RATIO:
                return pruningByMaxRatio(sparseVector, pruneRatio);
            case ABS_VALUE:
                return pruningByValue(sparseVector, pruneRatio);
            default:
                return sparseVector;
        }
    }

    /**
     * Validates whether a prune ratio is valid for a given pruning type.
     *
     * @param pruningType The type of pruning strategy
     * @param pruneRatio The ratio or threshold to validate
     * @return true if the ratio is valid for the given pruning type, false otherwise
     * @throws IllegalArgumentException if pruning type is null
     */
    public static boolean isValidPruneRatio(PruningType pruningType, float pruneRatio) {
        if (pruningType == null) {
            throw new IllegalArgumentException("Pruning type cannot be null");
        }

        switch (pruningType) {
            case TOP_K:
                return pruneRatio > 0 && pruneRatio == Math.floor(pruneRatio);
            case ALPHA_MASS:
            case MAX_RATIO:
                return pruneRatio > 0 && pruneRatio < 1;
            case ABS_VALUE:
                return pruneRatio > 0;
            default:
                return true;
        }
    }
}
