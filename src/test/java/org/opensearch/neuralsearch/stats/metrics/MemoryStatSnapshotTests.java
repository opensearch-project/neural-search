/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.metrics;

import lombok.SneakyThrows;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.neuralsearch.stats.common.StatSnapshot;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.util.TestUtils.DELTA_FOR_SCORE_ASSERTION;

public class MemoryStatSnapshotTests extends OpenSearchTestCase {

    public void testConstructor_withBuilder() {
        MetricStatName expectedStatName = MetricStatName.MEMORY_SPARSE_MEMORY_USAGE;
        boolean expectedIsAggregationMetric = true;
        double expectedValue = 0.0d;

        MemoryStatSnapshot snapshot = MemoryStatSnapshot.builder()
            .statName(expectedStatName)
            .isAggregationMetric(expectedIsAggregationMetric)
            .value(expectedValue)
            .build();

        assertEquals(expectedStatName, snapshot.getStatName());
        assertTrue(snapshot.isAggregationMetric());
        assertEquals(expectedValue, snapshot.getValue(), DELTA_FOR_SCORE_ASSERTION);
    }

    @SneakyThrows
    public void testConstructor_withStreamInput() {
        StreamInput streamInput = mock(StreamInput.class);

        MetricStatName expectedStatName = MetricStatName.MEMORY_SPARSE_MEMORY_USAGE;
        boolean expectedIsAggregationMetric = true;
        double expectedValue = 0.0d;
        when(streamInput.readEnum(MetricStatName.class)).thenReturn(expectedStatName);
        when(streamInput.readBoolean()).thenReturn(expectedIsAggregationMetric);
        when(streamInput.readDouble()).thenReturn(expectedValue);

        MemoryStatSnapshot snapshot = new MemoryStatSnapshot(streamInput);
        assertEquals(expectedStatName, snapshot.getStatName());
        assertEquals(expectedIsAggregationMetric, snapshot.isAggregationMetric());
        assertEquals(expectedValue, snapshot.getValue(), DELTA_FOR_SCORE_ASSERTION);
    }

    @SneakyThrows
    public void testWriteTo() {
        MetricStatName expectedStatName = MetricStatName.MEMORY_SPARSE_MEMORY_USAGE;
        boolean expectedIsAggregationMetric = true;
        double expectedValue = 0.0d;

        MemoryStatSnapshot snapshot = MemoryStatSnapshot.builder()
            .statName(expectedStatName)
            .isAggregationMetric(expectedIsAggregationMetric)
            .value(expectedValue)
            .build();

        StreamOutput streamOutput = mock(StreamOutput.class);
        snapshot.writeTo(streamOutput);
        verify(streamOutput).writeEnum(expectedStatName);
        verify(streamOutput).writeBoolean(expectedIsAggregationMetric);
        verify(streamOutput).writeDouble(expectedValue);
    }

    public void testAggregateMetricSnapshots() {
        MemoryStatSnapshot snapshot1 = MemoryStatSnapshot.builder()
            .statName(MetricStatName.MEMORY_SPARSE_MEMORY_USAGE)
            .value(100.0d)
            .build();
        MemoryStatSnapshot snapshot2 = MemoryStatSnapshot.builder()
            .statName(MetricStatName.MEMORY_SPARSE_MEMORY_USAGE)
            .value(200.0d)
            .build();
        MemoryStatSnapshot snapshot3 = MemoryStatSnapshot.builder()
            .statName(MetricStatName.MEMORY_SPARSE_MEMORY_USAGE)
            .value(300.0d)
            .build();

        List<MemoryStatSnapshot> snapshots = List.of(snapshot1, snapshot2, snapshot3);
        MemoryStatSnapshot result = MemoryStatSnapshot.aggregateMetricSnapshots(snapshots);

        assertNotNull(result);
        assertEquals(MetricStatName.MEMORY_SPARSE_MEMORY_USAGE, result.getStatName());
        assertEquals(600.0d, result.getValue(), DELTA_FOR_SCORE_ASSERTION);
    }

    public void testAggregateMetricSnapshots_withEmptyList() {
        MemoryStatSnapshot result = MemoryStatSnapshot.aggregateMetricSnapshots(List.of());

        assertNull(result);
    }

    public void testAggregateMetricSnapshots_withNullStat() {
        double expectedValue = 0.0d;
        Set<MemoryStatSnapshot> memoryStatSnapshotCollection = new HashSet<>();
        memoryStatSnapshotCollection.add(null);

        MemoryStatSnapshot result = MemoryStatSnapshot.aggregateMetricSnapshots(memoryStatSnapshotCollection);

        assertNotNull(result);
        assertNull(result.getStatName());
        assertFalse(result.isAggregationMetric());
        assertEquals(expectedValue, result.getValue(), DELTA_FOR_SCORE_ASSERTION);
    }

    public void testAggregateMetricSnapshots_withDifferentStatName() {
        MemoryStatSnapshot snapshot1 = MemoryStatSnapshot.builder()
            .statName(MetricStatName.MEMORY_SPARSE_MEMORY_USAGE)
            .value(100.0d)
            .build();
        MemoryStatSnapshot snapshot2 = MemoryStatSnapshot.builder()
            .statName(MetricStatName.MEMORY_SPARSE_FORWARD_INDEX_USAGE)
            .value(200.0d)
            .build();

        List<MemoryStatSnapshot> snapshots = List.of(snapshot1, snapshot2);
        Exception exception = assertThrows(IllegalArgumentException.class, () -> MemoryStatSnapshot.aggregateMetricSnapshots(snapshots));
        assertEquals("Should not aggregate snapshots across different stat names", exception.getMessage());
    }

    @SneakyThrows
    public void testToXContent() {
        MetricStatName expectedStatName = MetricStatName.MEMORY_SPARSE_MEMORY_USAGE;
        boolean expectedIsAggregationMetric = true;
        Double expectedValue = 1.0d;

        MemoryStatSnapshot snapshot = MemoryStatSnapshot.builder()
            .statName(expectedStatName)
            .isAggregationMetric(expectedIsAggregationMetric)
            .value(expectedValue)
            .build();

        XContentBuilder builder = mock(XContentBuilder.class);
        ToXContent.Params params = mock(ToXContent.Params.class);
        snapshot.toXContent(builder, params);

        verify(builder).field(StatSnapshot.VALUE_FIELD, expectedValue);
        verify(builder).field(StatSnapshot.STAT_TYPE_FIELD, "memory");
    }
}
