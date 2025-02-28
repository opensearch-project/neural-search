/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.state;

import lombok.Getter;
import lombok.Setter;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.neuralsearch.stats.common.StatSnapshot;

import java.io.IOException;

public class SettableStateStatSnapshot<T> implements StatSnapshot<T> {
    @Getter
    @Setter
    private T value;

    private StateStatName statName;

    public SettableStateStatSnapshot() {
        this.value = null;
    }

    public SettableStateStatSnapshot(StateStatName statName, T value) {
        this.statName = statName;
        this.value = value;
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
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(StatSnapshot.VALUE_KEY, getValue());
        builder.field(StatSnapshot.STAT_TYPE_KEY, statName.getStatType().getName());
        builder.endObject();
        return builder;
    }
}
