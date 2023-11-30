/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.settings;

import org.opensearch.common.settings.Setting;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Class defines settings specific to neural-search plugin
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class NeuralSearchSettings {

    /**
     * Gates the functionality of hybrid search
     * Currently query phase searcher added with hybrid search will conflict with concurrent search in core.
     * Once that problem is resolved this feature flag can be removed.
     */
    public static final Setting<Boolean> NEURAL_SEARCH_HYBRID_SEARCH_DISABLED = Setting.boolSetting(
        "plugins.neural_search.hybrid_search_disabled",
        false,
        Setting.Property.NodeScope
    );
}
