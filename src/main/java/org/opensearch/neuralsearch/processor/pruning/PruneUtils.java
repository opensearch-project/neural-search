/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.pruning;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PruneUtils {
    public static final String PRUNE_TYPE_FIELD = "prune_type";
    public static final String PRUNE_RATIO_FIELD = "prune_ratio";

    public static Map<String, Float> pruningByTopK(Map<String, Float> sparseVector, int k) {
        List<Map.Entry<String, Float>> list = new ArrayList<>(sparseVector.entrySet());
        list.sort(Map.Entry.comparingByValue(Comparator.reverseOrder()));

        Map<String, Float> result = new HashMap<>();
        for (int i = 0; i < k && i < list.size(); i++) {
            Map.Entry<String, Float> entry = list.get(i);
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

    public static Map<String, Float> pruningByMaxRatio(Map<String, Float> sparseVector, float ratio) {
        float maxValue = sparseVector.values().stream().max(Float::compareTo).orElse(0f);

        Map<String, Float> result = new HashMap<>();
        for (Map.Entry<String, Float> entry : sparseVector.entrySet()) {
            float currentValue = entry.getValue();
            float currentRatio = currentValue / maxValue;

            if (currentRatio >= ratio) {
                result.put(entry.getKey(), entry.getValue());
            }
        }

        return result;
    }

    public static Map<String, Float> pruningByValue(Map<String, Float> sparseVector, float thresh) {
        Map<String, Float> result = new HashMap<>(sparseVector);
        for (Map.Entry<String, Float> entry : sparseVector.entrySet()) {
            float currentValue = Math.abs(entry.getValue());
            if (currentValue < thresh) {
                result.remove(entry.getKey());
            }
        }

        return result;
    }

    public static Map<String, Float> pruningByAlphaMass(Map<String, Float> sparseVector, float alpha) {
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

    public static Map<String, Float> pruningSparseVector(PruningType pruningType, float pruneRatio, Map<String, Float> sparseVector) {
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
}
