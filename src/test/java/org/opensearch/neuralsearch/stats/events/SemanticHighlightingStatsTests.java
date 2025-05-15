/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.events;

import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.util.EnumSet;
import java.util.Locale;

public class SemanticHighlightingStatsTests extends OpenSearchTestCase {
    private static final EnumSet<EventStatName> SEMANTIC_HIGHLIGHTING_STATS = EnumSet.of(
        EventStatName.SEMANTIC_HIGHLIGHTING_REQUEST_COUNT,
        EventStatName.SEMANTIC_HIGHLIGHTING_ERROR_COUNT
    );

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    public void testSemanticHighlightingStatsExist() {
        // Verify that both semantic highlighting stats exist in the enum
        assertTrue(
            "SEMANTIC_HIGHLIGHTING_REQUEST_COUNT should exist",
            EnumSet.allOf(EventStatName.class).contains(EventStatName.SEMANTIC_HIGHLIGHTING_REQUEST_COUNT)
        );
        assertTrue(
            "SEMANTIC_HIGHLIGHTING_ERROR_COUNT should exist",
            EnumSet.allOf(EventStatName.class).contains(EventStatName.SEMANTIC_HIGHLIGHTING_ERROR_COUNT)
        );
    }

    public void testSemanticHighlightingStatsPaths() {
        // Verify that both stats have the correct path
        assertEquals(
            "SEMANTIC_HIGHLIGHTING_REQUEST_COUNT should have correct path",
            "semantic_highlighting",
            EventStatName.SEMANTIC_HIGHLIGHTING_REQUEST_COUNT.getPath()
        );
        assertEquals(
            "SEMANTIC_HIGHLIGHTING_ERROR_COUNT should have correct path",
            "semantic_highlighting",
            EventStatName.SEMANTIC_HIGHLIGHTING_ERROR_COUNT.getPath()
        );
    }

    public void testSemanticHighlightingStatsNames() {
        // Verify that both stats have the correct name strings
        assertEquals(
            "SEMANTIC_HIGHLIGHTING_REQUEST_COUNT should have correct name",
            "request_count",
            EventStatName.SEMANTIC_HIGHLIGHTING_REQUEST_COUNT.getNameString()
        );
        assertEquals(
            "SEMANTIC_HIGHLIGHTING_ERROR_COUNT should have correct name",
            "error_count",
            EventStatName.SEMANTIC_HIGHLIGHTING_ERROR_COUNT.getNameString()
        );
    }

    public void testSemanticHighlightingStatsFullPaths() {
        // Verify that both stats have the correct full paths
        assertEquals(
            "SEMANTIC_HIGHLIGHTING_REQUEST_COUNT should have correct full path",
            "semantic_highlighting.request_count",
            EventStatName.SEMANTIC_HIGHLIGHTING_REQUEST_COUNT.getFullPath()
        );
        assertEquals(
            "SEMANTIC_HIGHLIGHTING_ERROR_COUNT should have correct full path",
            "semantic_highlighting.error_count",
            EventStatName.SEMANTIC_HIGHLIGHTING_ERROR_COUNT.getFullPath()
        );
    }

    public void testSemanticHighlightingStatsTypes() {
        // Verify that both stats have the correct type
        for (EventStatName stat : SEMANTIC_HIGHLIGHTING_STATS) {
            assertEquals("Stat should be of type TIMESTAMPED_EVENT_COUNTER", EventStatType.TIMESTAMPED_EVENT_COUNTER, stat.getStatType());
        }
    }

    public void testSemanticHighlightingStatsEventStats() {
        // Verify that both stats have non-null event stats
        for (EventStatName stat : SEMANTIC_HIGHLIGHTING_STATS) {
            assertNotNull("Event stat should not be null", stat.getEventStat());
            assertTrue("Event stat should be instance of TimestampedEventStat", stat.getEventStat() instanceof TimestampedEventStat);
        }
    }

    public void testSemanticHighlightingStatsFromString() {
        // Test looking up stats by their name strings
        assertEquals(
            "Should find SEMANTIC_HIGHLIGHTING_REQUEST_COUNT by name",
            EventStatName.SEMANTIC_HIGHLIGHTING_REQUEST_COUNT,
            EventStatName.from("request_count")
        );
        assertEquals(
            "Should find SEMANTIC_HIGHLIGHTING_ERROR_COUNT by name",
            EventStatName.SEMANTIC_HIGHLIGHTING_ERROR_COUNT,
            EventStatName.from("error_count")
        );

        // Test invalid name
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> EventStatName.from("invalid_stat_name"));
        assertTrue(
            "Error message should mention invalid stat name",
            exception.getMessage().toLowerCase(Locale.ROOT).contains("event stat not found")
        );
    }
}
