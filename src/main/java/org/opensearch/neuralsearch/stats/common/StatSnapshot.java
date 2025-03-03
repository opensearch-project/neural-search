/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.common;

import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * A serializable snapshot of a stat at a given point in time.
 * Holds stat values, type, and metadata for processing and returning across rest layer.
 * These are not meant to be persisted.
 * @param <T> The type of the value of the stat
 */
public interface StatSnapshot<T> extends ToXContentFragment {
    /**
     * Field name of the stat_type in XContent
     */
    String STAT_TYPE_FIELD = "stat_type";

    /**
     * Field name of the value in XContent
     */
    String VALUE_FIELD = "value";

    /**
     * Gets the raw value of the stat, excluding any metadata
     * @return the raw stat value
     */
    T getValue();

    /**
     * Converts to fields xContent, including stat metadata
     *
     * @param builder XContentBuilder
     * @param params Params
     * @return XContentBuilder
     * @throws IOException thrown by builder for invalid field
     */
    XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException;
}
