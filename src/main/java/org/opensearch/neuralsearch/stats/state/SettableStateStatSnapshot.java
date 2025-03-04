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

/**
 * A settable state snapshot used to track Strings, booleans, or other simple serializable objects
 * This are meant to be constructed, set, and serialized, not for long storage in memory
 * @param <T> the type of the value to set
 */
public class SettableStateStatSnapshot<T> implements StatSnapshot<T> {
    @Getter
    @Setter
    private T value;

    private StateStatName statName;

    /**
     * Creates a new stat snapshot with default null value
     * @param statName the associated stat name
     */
    public SettableStateStatSnapshot(StateStatName statName) {
        this.statName = statName;
        this.value = null;
    }

    /**
     * Creates a new stat snapshot for a given value
     * @param statName the associated stat name
     * @param value the initial value to set
     */
    public SettableStateStatSnapshot(StateStatName statName, T value) {
        this.statName = statName;
        this.value = value;
    }

    /**
     * Converts to fields xContent, including stat metadata
     *
     * @param builder XContentBuilder
     * @param params Params
     * @return XContentBuilder
     * @throws IOException thrown by builder for invalid field
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(StatSnapshot.VALUE_FIELD, getValue());
        builder.field(StatSnapshot.STAT_TYPE_FIELD, statName.getStatType().getTypeString());
        builder.endObject();
        return builder;
    }
}
