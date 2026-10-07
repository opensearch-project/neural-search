/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.info;

import org.opensearch.neuralsearch.rest.RestNeuralStatsAction;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

public class InfoStatNameTests extends OpenSearchTestCase {
    public static final EnumSet<InfoStatName> INFO_STATS = EnumSet.allOf(InfoStatName.class);

    public void test_fromValid() {
        String validStatName = InfoStatName.TEXT_EMBEDDING_PROCESSORS.getNameString();
        InfoStatName result = InfoStatName.from(validStatName);
        assertEquals(InfoStatName.TEXT_EMBEDDING_PROCESSORS, result);
    }

    public void test_fromInvalid() {
        assertThrows(IllegalArgumentException.class, () -> { InfoStatName.from("non_existent_stat"); });
    }

    /**
     * The ordinal of an info stat is its wire format, so this list is frozen: a new stat goes at the end of the enum and
     * at the end here. Inserting or reordering one is what breaks a mixed cluster - the coordinating node then sends the
     * older node an ordinal that node reads as a different stat, or cannot read at all.
     */
    public void test_ordinalsAreFrozen() {
        List<String> frozenOrder = List.of(
            "CLUSTER_VERSION",
            "TEXT_EMBEDDING_PROCESSORS",
            "SKIP_EXISTING_PROCESSORS",
            "TEXT_CHUNKING_PROCESSORS",
            "TEXT_CHUNKING_DELIMITER_PROCESSORS",
            "TEXT_CHUNKING_FIXED_TOKEN_LENGTH_PROCESSORS",
            "TEXT_CHUNKING_FIXED_CHAR_LENGTH_PROCESSORS",
            "NORMALIZATION_PROCESSORS",
            "NORM_TECHNIQUE_L2_PROCESSORS",
            "NORM_TECHNIQUE_MINMAX_PROCESSORS",
            "NORM_TECHNIQUE_ZSCORE_PROCESSORS",
            "COMB_TECHNIQUE_ARITHMETIC_PROCESSORS",
            "COMB_TECHNIQUE_GEOMETRIC_PROCESSORS",
            "COMB_TECHNIQUE_HARMONIC_PROCESSORS",
            "RRF_PROCESSORS",
            "COMB_TECHNIQUE_RRF_PROCESSORS",
            "TEXT_IMAGE_EMBEDDING_PROCESSORS",
            "SPARSE_ENCODING_PROCESSORS",
            "NEURAL_QUERY_ENRICHER_PROCESSORS",
            "NEURAL_SPARSE_TWO_PHASE_PROCESSORS",
            "RERANK_BY_FIELD_PROCESSORS",
            "RERANK_ML_PROCESSORS",
            "AGENTIC_QUERY_TRANSLATOR_PROCESSORS",
            "AGENTIC_CONTEXT_PROCESSORS",
            "SPARSE_VECTOR_INDICES",
            "SPARSE_VECTOR_FIELDS",
            "SPARSE_NATIVE_ENGINE_INDICES",
            "SPARSE_NATIVE_ENGINE_FIELDS",
            // Resolver (in-query `fusion`) mode: the enabled-vs-used funnel. Appended, never inserted.
            "HYBRID_FUSION_ENABLED"
        );

        assertEquals(frozenOrder, Arrays.stream(InfoStatName.values()).map(Enum::name).toList());
    }

    public void test_validNames() {
        Set<String> names = new HashSet<>();
        for (InfoStatName statName : INFO_STATS) {
            String name = statName.getNameString().toLowerCase(Locale.ROOT);
            assertFalse(String.format(Locale.ROOT, "Checking name uniqueness for %s", name), names.contains(name));
            assertTrue(RestNeuralStatsAction.isValidParamString(name));
            names.add(name);
        }
    }

    public void test_uniquePaths() {
        Set<String> paths = new HashSet<>();

        // First pass to add all base paths (excluding stat names) to avoid colliding a stat name with a terminal path
        // e.g. if a.b is a stat, a.b.c cannot be a stat.
        for (InfoStatName statName : INFO_STATS) {
            String path = statName.getPath().toLowerCase(Locale.ROOT);
            paths.add(path);
        }

        // Check possible path collisions
        // i.e. a full path is a terminal path that should not have any children
        for (InfoStatName statName : INFO_STATS) {
            String path = statName.getFullPath().toLowerCase(Locale.ROOT);
            assertFalse(String.format(Locale.ROOT, "Checking full path uniqueness for %s", path), paths.contains(path));
            paths.add(path);
        }
    }

    /**
     * Tests if there are any path prefix collisions
     * i.e. every full stat path should be terminal.
     * There should be no other paths that start with another full stat path
     */
    public void test_noPathCollisions() {
        // Convert paths to list and sort them
        List<String> sortedPaths = new ArrayList<>();
        for (InfoStatName stat : INFO_STATS) {
            sortedPaths.add(stat.getFullPath().toLowerCase(Locale.ROOT));
        }
        sortedPaths.sort(String::compareTo);

        // Check adjacent paths for collisions
        // When sorted alphabetically, we can reduce the number of path collision comparisons
        for (int i = 0; i < sortedPaths.size() - 1; i++) {
            String currentPath = sortedPaths.get(i);
            String nextPath = sortedPaths.get(i + 1);

            // Check for prefix collision
            assertFalse(
                String.format(Locale.ROOT, "Path collision found: %s is a prefix of %s", currentPath, nextPath),
                isPathPrefixOf(currentPath, nextPath)
            );
        }
    }

    private boolean isPathPrefixOf(String path1, String path2) {
        if (path2.startsWith(path1)) {
            if (path1.length() == path2.length()) {
                return false;
            }
            return path2.charAt(path1.length()) == '.';
        }
        return false;
    }
}
