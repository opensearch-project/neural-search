/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.common;

import org.opensearch.core.xcontent.ToXContentFragment;

public interface StatSnapshot<T> extends ToXContentFragment {
    public static final String STAT_TYPE_KEY = "stat_type";
    public static final String VALUE_KEY = "value";

    T getValue();
}
