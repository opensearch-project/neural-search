/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.events;

import org.junit.Before;
import org.mockito.Spy;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

public class TimestampedEventStatTests extends OpenSearchTestCase {
    private static final long BUCKET_INTERVAL_MS = 60 * 1000; // 60 seconds
    private static final EventStatName STAT_NAME = EventStatName.TEXT_EMBEDDING_PROCESSOR_EXECUTIONS;

    @Spy
    private TimestampedEventStat stat;

    private long currentTime;

    @Before
    public void setup() {
        stat = spy(new TimestampedEventStat(STAT_NAME));
        currentTime = System.currentTimeMillis();
        doAnswer(inv -> currentTime).when(stat).getCurrentTimeInMillis();
    }

    public void test_initialization() {
        assertEquals(0, stat.getValue());
        assertEquals(0, stat.getTrailingIntervalValue());
        assertNotEquals(0, stat.getMinutesSinceLastEvent());
    }

    public void test_basicIncrement() {
        stat.increment();
        assertEquals(1, stat.getValue());

        stat.increment();
        assertEquals(2, stat.getValue());
    }

    public void test_trailingIntervalSingleBucket() {
        // Add events in same bucket
        for (int i = 0; i < 5; i++) {
            stat.increment();
        }

        // Should not count current bucket
        assertEquals(0, stat.getTrailingIntervalValue());

        // Move to next bucket
        currentTime += BUCKET_INTERVAL_MS;
        assertEquals(5, stat.getTrailingIntervalValue());
    }

    public void test_trailingIntervalMultipleBuckets() {
        // Add events across multiple buckets
        stat.increment(); // Bucket 1
        currentTime += BUCKET_INTERVAL_MS;

        stat.increment(); // Bucket 2
        stat.increment();
        currentTime += BUCKET_INTERVAL_MS;

        stat.increment(); // Bucket 3
        currentTime += BUCKET_INTERVAL_MS;

        assertEquals(4, stat.getValue());
        assertEquals(4, stat.getTrailingIntervalValue());
    }

    public void test_bucketRotation() {
        // Fill buckets across 10 minutes
        for (int i = 0; i < 10; i++) {
            stat.increment();
            currentTime += BUCKET_INTERVAL_MS;
        }

        // Should drop oldest buckets
        assertEquals(10, stat.getValue());
        assertEquals(5, stat.getTrailingIntervalValue());
    }

    public void test_minutesSinceLastEvent() {
        stat.increment();
        assertEquals(0, stat.getMinutesSinceLastEvent());

        currentTime += TimeUnit.MINUTES.toMillis(5);
        assertEquals(5, stat.getMinutesSinceLastEvent());
    }

    public void test_reset() {
        stat.increment();
        stat.increment();
        currentTime += BUCKET_INTERVAL_MS;
        stat.increment();

        stat.reset();

        assertEquals(0, stat.getValue());
        assertEquals(0, stat.getTrailingIntervalValue());
        assertNotEquals(0, stat.getMinutesSinceLastEvent());
    }

    public void test_eventStatSnapshot() {
        stat.increment();
        currentTime += BUCKET_INTERVAL_MS * 2;
        stat.increment();
        stat.increment();
        currentTime += BUCKET_INTERVAL_MS * 2;

        TimestampedEventStatSnapshot snapshot = stat.getStatSnapshot();
        assertEquals(3, snapshot.getValue().longValue());
        assertEquals(3, stat.getTrailingIntervalValue());
        assertEquals(2, snapshot.getMinutesSinceLastEvent());
    }

    public void test_longTimeGap() {
        stat.increment();
        stat.increment();

        // Simulate a very long time gap
        currentTime += TimeUnit.DAYS.toMillis(1);

        assertEquals(0, stat.getTrailingIntervalValue());
        assertEquals(24 * 60, stat.getMinutesSinceLastEvent());
    }
}
