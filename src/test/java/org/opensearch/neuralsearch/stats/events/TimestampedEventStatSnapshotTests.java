/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.events;

import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.util.TestUtils.xContentBuilderToMap;

public class TimestampedEventStatSnapshotTests extends OpenSearchTestCase {
    private static final EventStatName STAT_NAME = EventStatName.TEXT_EMBEDDING_PROCESSOR_EXECUTIONS;

    public void test_constructorAndGetters() {
        TimestampedEventStatSnapshot snapshot = new TimestampedEventStatSnapshot(STAT_NAME, 100L, 50L, 10L);

        assertEquals(STAT_NAME, snapshot.getStatName());
        assertEquals(100L, snapshot.getValue().longValue());
        assertEquals(50L, snapshot.getTrailingIntervalValue());
        assertEquals(10L, snapshot.getMinutesSinceLastEvent());
    }

    public void test_streamConstructor() throws IOException {
        StreamInput mockInput = mock(StreamInput.class);
        when(mockInput.readEnum(EventStatName.class)).thenReturn(STAT_NAME);
        when(mockInput.readLong()).thenReturn(100L, 50L, 10L);

        TimestampedEventStatSnapshot snapshot = new TimestampedEventStatSnapshot(mockInput);

        assertEquals(STAT_NAME, snapshot.getStatName());
        assertEquals(100L, snapshot.getValue().longValue());
        assertEquals(50L, snapshot.getTrailingIntervalValue());
        assertEquals(10L, snapshot.getMinutesSinceLastEvent());

        verify(mockInput, times(1)).readEnum(EventStatName.class);
        verify(mockInput, times(3)).readLong();
    }

    public void test_writeToOutputs() throws IOException {
        TimestampedEventStatSnapshot snapshot = new TimestampedEventStatSnapshot(STAT_NAME, 100L, 50L, 10L);

        StreamOutput mockOutput = mock(StreamOutput.class);
        snapshot.writeTo(mockOutput);

        verify(mockOutput).writeEnum(STAT_NAME);
        verify(mockOutput).writeLong(100L);
        verify(mockOutput).writeLong(50L);
        verify(mockOutput).writeLong(10L);
    }

    public void test_aggregateEventStatSnapshots() {
        TimestampedEventStatSnapshot snapshot1 = new TimestampedEventStatSnapshot(STAT_NAME, 100L, 50L, 10L);
        TimestampedEventStatSnapshot snapshot2 = new TimestampedEventStatSnapshot(STAT_NAME, 200L, 100L, 5L);

        TimestampedEventStatSnapshot aggregatedSnapshot = TimestampedEventStatSnapshot.aggregateEventStatSnapshots(
            Arrays.asList(snapshot1, snapshot2)
        );

        assertEquals(STAT_NAME, aggregatedSnapshot.getStatName());
        assertEquals(300L, aggregatedSnapshot.getValue().longValue());
        assertEquals(150L, aggregatedSnapshot.getTrailingIntervalValue());
        assertEquals(5L, aggregatedSnapshot.getMinutesSinceLastEvent());
    }

    public void test_aggregateEventStatSnapshotsReturnsNull() {
        assertNull(TimestampedEventStatSnapshot.aggregateEventStatSnapshots(Collections.emptyList()));
    }

    public void test_aggregateEventStatDataThrowsException() {
        TimestampedEventStatSnapshot snapshot1 = new TimestampedEventStatSnapshot(STAT_NAME, 100L, 50L, 10L);
        TimestampedEventStatSnapshot snapshot2 = new TimestampedEventStatSnapshot(null, 200L, 100L, 5L);

        assertThrows(
            IllegalArgumentException.class,
            () -> TimestampedEventStatSnapshot.aggregateEventStatSnapshots(Arrays.asList(snapshot1, snapshot2))
        );
    }

    public void test_toXContent() throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        TimestampedEventStatSnapshot snapshot = new TimestampedEventStatSnapshot(STAT_NAME, 100L, 50L, 10L);

        snapshot.toXContent(builder, null);

        Map<String, Object> responseMap = xContentBuilderToMap(builder);

        assertEquals(100, responseMap.get("value"));
        assertEquals(50, responseMap.get("trailing_interval_value"));
        assertEquals(10, responseMap.get("minutes_since_last_event"));
        assertEquals(STAT_NAME.getStatType().getTypeString(), responseMap.get("stat_type"));
    }
}
