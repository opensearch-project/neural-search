/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.events;

import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.neuralsearch.settings.NeuralSearchSettingsAccessor;
import org.opensearch.test.OpenSearchTestCase;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class EventStatsManagerTests extends OpenSearchTestCase {
    private static final EventStatName STAT_NAME = EventStatName.TEXT_EMBEDDING_PROCESSOR_EXECUTIONS;

    @Mock
    private NeuralSearchSettingsAccessor mockSettingsAccessor;

    @Mock
    private TimestampedEventStat mockEventStat;

    private EventStatsManager eventStatsManager;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        eventStatsManager = new EventStatsManager(mockSettingsAccessor);
    }

    public void test_constructorInitializesAllEventStats() {
        EventStatsManager manager = new EventStatsManager(mockSettingsAccessor);
        Map<EventStatName, EventStat> stats = manager.getStats();

        assertFalse(stats.isEmpty());

        Set<EventStatName> trackedEventStatNames = stats.keySet();
        Set<EventStatName> expectedEventStatNames = EnumSet.allOf(EventStatName.class);

        assertEquals(expectedEventStatNames, trackedEventStatNames);
    }

    public void test_increment() {
        when(mockSettingsAccessor.getIsStatsEnabled()).thenReturn(true);

        EventStat originalStat = eventStatsManager.getStats().get(STAT_NAME);
        long originalValue = originalStat.getValue();

        eventStatsManager.inc(STAT_NAME);

        long newValue = originalStat.getValue();
        assertEquals(originalValue + 1, newValue);
    }

    public void test_incrementWhenStatsDisabled() {
        when(mockSettingsAccessor.getIsStatsEnabled()).thenReturn(false);

        EventStat originalStat = eventStatsManager.getStats().get(STAT_NAME);
        long originalValue = originalStat.getValue();

        eventStatsManager.inc(STAT_NAME);

        long newValue = originalStat.getValue();
        assertEquals(originalValue, newValue);
    }

    public void test_getTimestampedEventStatSnapshots() {
        TimestampedEventStatSnapshot mockSnapshot = mock(TimestampedEventStatSnapshot.class);
        when(mockEventStat.getStatSnapshot()).thenReturn(mockSnapshot);

        eventStatsManager.getStats().put(STAT_NAME, mockEventStat);

        Map<EventStatName, TimestampedEventStatSnapshot> result = eventStatsManager.getTimestampedEventStatSnapshots(EnumSet.of(STAT_NAME));

        assertEquals(1, result.size());
        assertEquals(mockSnapshot, result.get(STAT_NAME));
        verify(mockEventStat, times(1)).getStatSnapshot();
    }

    public void test_getTimestampedEventStatSnapshotsReturnsEmptyMap() {
        Map<EventStatName, TimestampedEventStatSnapshot> result = eventStatsManager.getTimestampedEventStatSnapshots(
            EnumSet.noneOf(EventStatName.class)
        );

        assertTrue(result.isEmpty());
    }

    public void test_reset() {
        // Create multiple mock stats
        TimestampedEventStat mockStat1 = mock(TimestampedEventStat.class);

        // Add mock stats to manager
        eventStatsManager.getStats().clear();
        eventStatsManager.getStats().put(STAT_NAME, mockStat1);

        eventStatsManager.reset();

        verify(mockStat1, times(1)).reset();
    }

    public void test_singletonInstanceCreation() {
        // Test that multiple calls return same instance
        EventStatsManager instance1 = EventStatsManager.instance();
        EventStatsManager instance2 = EventStatsManager.instance();

        assertNotNull(instance1);
        assertSame(instance1, instance2);
    }
}
