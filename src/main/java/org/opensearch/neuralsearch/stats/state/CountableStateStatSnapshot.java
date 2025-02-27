/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.state;

import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.neuralsearch.stats.StatSnapshot;

import java.io.IOException;
import java.util.concurrent.atomic.LongAdder;

public class CountableStateStatSnapshot implements StatSnapshot<Long> {
    private LongAdder adder;
    private StateStatName statName;

    public CountableStateStatSnapshot(StateStatName statName) {
        this.statName = statName;
        this.adder = new LongAdder();
    }

    public Long getValue() {
        return adder.longValue();
    }

    public void increment() {
        adder.increment();
    }

    public void incrementBy(Long delta) {
        adder.add(delta);
    }

    /**
     * Converts to fields xContent
     *
     * @param builder XContentBuilder
     * @param params Params
     * @return XContentBuilder
     * @throws IOException thrown by builder for invalid field
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field(StatSnapshot.VALUE_KEY, getValue());
        builder.field(StatSnapshot.STAT_TYPE_KEY, statName.getStatType().name());
        builder.endObject();
        return builder;
    }
}
