/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.events;

import org.opensearch.neuralsearch.rest.RestNeuralStatsAction;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
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
