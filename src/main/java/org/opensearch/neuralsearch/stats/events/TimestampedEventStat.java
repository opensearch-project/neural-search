/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.events;

import com.google.common.annotations.VisibleForTesting;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Event stat information tracker which store and updates ongoing event stat data and metadata
 * Tracks a single monotonically increasing counter, a unix timestamp of the last event, and a value of the counter
 * in a recent trailing interval of time defined by the constants
 */
public class TimestampedEventStat implements EventStat {
    // The length of the rotating time bucket used to track the trailing interval
    // Trailing interval size is determined by interval size * number of intervals
    private static final long TRAILING_BUCKET_INTERVAL_MS = TimeUnit.SECONDS.toMillis(60);

    // Number of buckets to track for the trailing interval
    private static final int TRAILING_NUMBER_OF_INTERVALS = 5;

    private EventStatName statName;
    private long lastEventTimestamp;
    private LongAdder totalCounter;
    private Bucket[] buckets;

    /**
     * Constructor
     * @param statName the associate stat name identifier
     */
    public TimestampedEventStat(EventStatName statName) {
        this.statName = statName;
        this.lastEventTimestamp = 0L;
        this.totalCounter = new LongAdder();
        this.buckets = new Bucket[TRAILING_NUMBER_OF_INTERVALS + 1];

        for (int i = 0; i < TRAILING_NUMBER_OF_INTERVALS + 1; i++) {
            buckets[i] = new Bucket();
        }
    }

    /**
     * Gets the current counter value
     * @return
     */
    public long getValue() {
        return totalCounter.longValue();
    }

    /**
     * Increments the counter
     */
    public void increment() {
        totalCounter.increment();
        lastEventTimestamp = getCurrentTimeInMillis();
        incrementCurrentBucket();
    }

    /**
     * Helper to increment the current bucket based on system time
     */
    private void incrementCurrentBucket() {
        long now = getCurrentTimeInMillis();

        // Align current time to current minute
        long currentBucketTime = now - (now % TRAILING_BUCKET_INTERVAL_MS);

        // Use aligned time to determine bucket index
        int bucketIndex = (int) ((now / TRAILING_BUCKET_INTERVAL_MS) % (TRAILING_NUMBER_OF_INTERVALS + 1));

        Bucket bucket = buckets[bucketIndex];
        long bucketTimestamp = bucket.timestamp.get();

        // If bucket is out of date, rotate the bucket timestamp and reset the bucket
        if (bucketTimestamp != currentBucketTime && bucket.timestamp.compareAndSet(bucketTimestamp, currentBucketTime)) {
            bucket.count.reset();
        }
        bucket.count.add(1);
    }

    /**
     * Gets the current count value of the trailing interval
     * @return the total count of all events in the recent trailing interval
     */
    public long getTrailingIntervalValue() {
        long now = getCurrentTimeInMillis();
        long currentBucketTime = now - (now % TRAILING_BUCKET_INTERVAL_MS); // Start of current minute

        long cutoff = now - (TRAILING_NUMBER_OF_INTERVALS * TRAILING_BUCKET_INTERVAL_MS); // Cutoff is number of buckets away
        long alignedCutoff = (cutoff / TRAILING_BUCKET_INTERVAL_MS) * TRAILING_BUCKET_INTERVAL_MS; // Align cutoff to bucket boundary

        long sum = 0;
        for (Bucket bucket : buckets) {
            long timestamp = bucket.timestamp.get();
            // Include buckets >= aligned cutoff and < current bucket (excludes current)
            if (timestamp >= alignedCutoff && timestamp < currentBucketTime) {
                sum += bucket.count.longValue();
            }
        }

        return sum;
    }

    /**
     * Gets the number of minutes since the last event
     * This is calculated relative to node system time to reduce time desync issues across different nodes
     * @return the number of minutes since the last event
     */
    public long getMinutesSinceLastEvent() {
        long currentTimestamp = getCurrentTimeInMillis();
        return (currentTimestamp / (1000 * 60)) - (lastEventTimestamp / (1000 * 60));
    }

    /**
     * Gets the StatSnapshot for the event stat data
     * @return
     */
    public TimestampedEventStatSnapshot getStatSnapshot() {
        return TimestampedEventStatSnapshot.builder()
            .statName(statName)
            .value(getValue())
            .trailingIntervalValue(getTrailingIntervalValue())
            .minutesSinceLastEvent(getMinutesSinceLastEvent())
            .build();
    }

    /**
     * Resets all stat data
     * Used when the cluster setting to enable stats is toggled off
     */
    public void reset() {
        for (int i = 0; i < buckets.length; i++) {
            buckets[i].timestamp.set(0);
            buckets[i].count.reset();
        }
        totalCounter.reset();
        lastEventTimestamp = 0;
    }

    /**
     * Helper class to get current time in millis. Abstracted for testing purposes
     * @return current time in millis
     */
    @VisibleForTesting
    protected long getCurrentTimeInMillis() {
        return System.currentTimeMillis();
    }

    /**
     * Private inner class for tracking trailing interval values
     */
    private class Bucket {
        AtomicLong timestamp = new AtomicLong(0); // Start time of the bucket's minute
        LongAdder count = new LongAdder(); // Running count of the bucket
    }
}
