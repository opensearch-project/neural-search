/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.events;

import org.opensearch.neuralsearch.rest.RestNeuralStatsAction;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

public class EventStatNameTests extends OpenSearchTestCase {
    public static final EnumSet<EventStatName> EVENT_STATS = EnumSet.allOf(EventStatName.class);

    public void test_fromValid() {
        String validStatName = EventStatName.TEXT_EMBEDDING_PROCESSOR_EXECUTIONS.getNameString();
        EventStatName result = EventStatName.from(validStatName);
        assertEquals(EventStatName.TEXT_EMBEDDING_PROCESSOR_EXECUTIONS, result);
    }

    public void test_fromInvalid() {
        assertThrows(IllegalArgumentException.class, () -> { EventStatName.from("non_existent_stat"); });
    }

    /**
     * The ordinal of an event stat is its wire format, so this list is frozen: a new stat goes at the end of the enum and
     * at the end here. Inserting or reordering one is what breaks a mixed cluster - the coordinating node then sends the
     * older node an ordinal that node reads as a different stat, or cannot read at all.
     */
    public void test_ordinalsAreFrozen() {
        List<String> frozenOrder = List.of(
            "TEXT_EMBEDDING_PROCESSOR_EXECUTIONS",
            "SKIP_EXISTING_EXECUTIONS",
            "TEXT_CHUNKING_PROCESSOR_EXECUTIONS",
            "TEXT_CHUNKING_FIXED_TOKEN_LENGTH_EXECUTIONS",
            "TEXT_CHUNKING_DELIMITER_EXECUTIONS",
            "TEXT_CHUNKING_FIXED_CHAR_LENGTH_EXECUTIONS",
            "SEMANTIC_FIELD_PROCESSOR_EXECUTIONS",
            "SEMANTIC_FIELD_PROCESSOR_CHUNKING_EXECUTIONS",
            "SEMANTIC_HIGHLIGHTING_REQUEST_COUNT",
            "SEMANTIC_HIGHLIGHTING_BATCH_REQUEST_COUNT",
            "NORMALIZATION_PROCESSOR_EXECUTIONS",
            "AGENTIC_QUERY_TRANSLATOR_PROCESSOR_EXECUTIONS",
            "AGENTIC_CONTEXT_PROCESSOR_EXECUTIONS",
            "NORM_TECHNIQUE_L2_EXECUTIONS",
            "NORM_TECHNIQUE_MINMAX_EXECUTIONS",
            "NORM_TECHNIQUE_NORM_ZSCORE_EXECUTIONS",
            "COMB_TECHNIQUE_ARITHMETIC_EXECUTIONS",
            "COMB_TECHNIQUE_GEOMETRIC_EXECUTIONS",
            "COMB_TECHNIQUE_HARMONIC_EXECUTIONS",
            "RRF_PROCESSOR_EXECUTIONS",
            "COMB_TECHNIQUE_RRF_EXECUTIONS",
            "HYBRID_QUERY_REQUESTS",
            "HYBRID_QUERY_INNER_HITS_REQUESTS",
            "HYBRID_QUERY_FILTER_REQUESTS",
            "HYBRID_QUERY_PAGINATION_REQUESTS",
            "NEURAL_QUERY_REQUESTS",
            "NEURAL_QUERY_AGAINST_KNN_REQUESTS",
            "NEURAL_QUERY_AGAINST_SEMANTIC_DENSE_REQUESTS",
            "NEURAL_QUERY_AGAINST_SEMANTIC_SPARSE_REQUESTS",
            "NEURAL_SPARSE_QUERY_REQUESTS",
            "TEXT_IMAGE_EMBEDDING_PROCESSOR_EXECUTIONS",
            "SPARSE_ENCODING_PROCESSOR_EXECUTIONS",
            "NEURAL_QUERY_ENRICHER_PROCESSOR_EXECUTIONS",
            "NEURAL_SPARSE_TWO_PHASE_PROCESSOR_EXECUTIONS",
            "RERANK_BY_FIELD_PROCESSOR_EXECUTIONS",
            "RERANK_ML_PROCESSOR_EXECUTIONS",
            "AGENTIC_QUERY_REQUESTS",
            "SEISMIC_QUERY_REQUESTS",
            "SPARSE_ENCODING_PROCESSOR_SEISMIC_EXECUTIONS",
            "MMR_NEURAL_QUERY_TRANSFORMER"
        );

        assertEquals(frozenOrder, Arrays.stream(EventStatName.values()).map(Enum::name).toList());
    }

    public void test_allEnumsHaveNonNullStats() {
        for (EventStatName statName : EVENT_STATS) {
            assertNotNull(statName.getEventStat());
        }
    }

    public void test_validNames() {
        Set<String> names = new HashSet<>();
        for (EventStatName statName : EVENT_STATS) {
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
        for (EventStatName statName : EVENT_STATS) {
            String path = statName.getPath().toLowerCase(Locale.ROOT);
            paths.add(path);
        }

        // Check possible path collisions
        // i.e. a full path is a terminal path that should not have any children
        for (EventStatName statName : EVENT_STATS) {
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
        for (EventStatName stat : EVENT_STATS) {
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
