/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import lombok.Getter;
import lombok.extern.log4j.Log4j2;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Log4j2
public class Profiling {
    private final List<ItemData> profilings = new ArrayList<>();

    public static final Profiling INSTANCE = new Profiling();
    private boolean run = false;

    public void run() {
        clear();
        run = true;
    }

    private Profiling() {
        for (ItemId itemId : ItemId.values()) {
            ItemData itemData = new ItemData();
            profilings.add(itemData);
        }
    }

    private void clear() {
        for (ItemId itemId : ItemId.values()) {
            ItemData itemData = new ItemData();
            itemData.clear();
        }
    }

    public long begin(ItemId itemId) {
        if (!run) {
            return 0;
        }
        return profilings.get(itemId.getId()).begin();
    }

    public void end(ItemId itemId, long start) {
        if (!run) {
            return;
        }
        profilings.get(itemId.getId()).end(start);
    }

    public void output() {
        for (ItemId itemId : ItemId.values()) {
            log.info("itemId: {}", itemId.name());
            profilings.get(itemId.getId()).output();
        }
    }

    public enum ItemId {
        DP(0),
        READ(1),
        VISITED(2),
        NEXTDOC(3),
        ACCEPTED(4),
        HEAP(5);

        @Getter
        private int id;

        ItemId(int i) {
            id = i;
        }
    }

    private static class ItemData {
        private AtomicInteger count = new AtomicInteger(0);
        private AtomicLong time = new AtomicLong(0);

        long begin() {
            return System.nanoTime();
        }

        void end(long startTime) {
            time.addAndGet(System.nanoTime() - startTime);
            count.incrementAndGet();
        }

        void clear() {
            count.set(0);
            time.set(0);
        }

        void output() {
            log.info("count: {}, avg time: {} ns", count, time.get() / count.get());
        }
    }
}
