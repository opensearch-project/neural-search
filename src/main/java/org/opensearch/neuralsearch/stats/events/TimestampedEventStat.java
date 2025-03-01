/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.events;

import com.google.common.annotations.VisibleForTesting;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public class TimestampedEventStat implements EventStat {
    private static final int INTERVAL_SIZE = 5; // N + 1 buckets to track N minutes (excludes current)
    private static final long BUCKET_INTERVAL_MS = TimeUnit.SECONDS.toMillis(60); // M millis per bucket

    private EventStatName statName;
    private long lastEventTimestamp;
    private LongAdder totalCounter;
    private Bucket[] buckets;

    public TimestampedEventStat(EventStatName statName) {
        this.statName = statName;
        this.lastEventTimestamp = 0L;
        this.totalCounter = new LongAdder();
        this.buckets = new Bucket[INTERVAL_SIZE + 1];

        for (int i = 0; i < INTERVAL_SIZE + 1; i++) {
            buckets[i] = new Bucket();
        }
    }

    public long getValue() {
        return totalCounter.longValue();
    }

    public void increment() {
        totalCounter.increment();
        lastEventTimestamp = getCurrentTimeInMillis();
        incrementCurrentBucket();
    }

    private void incrementCurrentBucket() {
        long now = getCurrentTimeInMillis();
        long currentBucketTime = now - (now % BUCKET_INTERVAL_MS); // Align to current minute
        int bucketIndex = (int) ((now / BUCKET_INTERVAL_MS) % (INTERVAL_SIZE + 1)); // Determine bucket index

        Bucket bucket = buckets[bucketIndex];
        long bucketTimestamp = bucket.timestamp.get();

        if (bucketTimestamp == currentBucketTime) {
            // Current minute's bucket: increment
            bucket.count.incrementAndGet();
        } else {
            // Bucket expired: reset if necessary, then increment
            if (bucket.timestamp.compareAndSet(bucketTimestamp, currentBucketTime)) {
                bucket.count.set(1); // Reset to 1 for the new minute
            } else {
                bucket.count.incrementAndGet(); // Another thread already reset; just increment
            }
        }
    }

    public long getTrailingIntervalValue() {
        long now = getCurrentTimeInMillis();
        long currentBucketTime = now - (now % BUCKET_INTERVAL_MS); // Start of current minute

        long cutoff = now - (INTERVAL_SIZE * BUCKET_INTERVAL_MS); // Cutoff is number of buckets away
        long alignedCutoff = (cutoff / BUCKET_INTERVAL_MS) * BUCKET_INTERVAL_MS; // Align cutoff to bucket boundary

        long sum = 0;
        for (Bucket bucket : buckets) {
            long timestamp = bucket.timestamp.get();
            // Include buckets >= aligned cutoff and < current bucket (excludes current)
            if (timestamp >= alignedCutoff && timestamp < currentBucketTime) {
                sum += bucket.count.get();
            }
        }

        return sum;
    }

    public long getMinutesSinceLastEvent() {
        long currentTimestamp = getCurrentTimeInMillis();
        return ((currentTimestamp - lastEventTimestamp) / (1000 * 60));
    }

    public TimestampedEventStatSnapshot getEventStatSnapshot() {
        return TimestampedEventStatSnapshot.builder()
            .statName(statName)
            .value(getValue())
            .trailingIntervalValue(getTrailingIntervalValue())
            .minutesSinceLastEvent(getMinutesSinceLastEvent())
            .build();
    }

    public void reset() {
        Bucket[] newBuckets = new Bucket[INTERVAL_SIZE + 1];
        for (int i = 0; i < INTERVAL_SIZE + 1; i++) {
            newBuckets[i] = new Bucket();
        }

        totalCounter.reset();
        synchronized (this) {
            buckets = newBuckets;
            lastEventTimestamp = 0;
        }
    }

    /**
     * Helper class to get current time in millis. Abstracted for testing purposes
     * @return current time in millis
     */
    @VisibleForTesting
    protected long getCurrentTimeInMillis() {
        return System.currentTimeMillis();
    }

    private class Bucket {
        AtomicLong timestamp = new AtomicLong(0); // Start time of the bucket's minute
        AtomicLong count = new AtomicLong(0);
    }
}
