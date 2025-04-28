/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import org.opensearch.common.settings.Setting;

import static org.opensearch.common.settings.Setting.Property.Final;
import static org.opensearch.common.settings.Setting.Property.IndexScope;

/**
 * It holds index settings of a sparse vector index.
 */
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class SparseSettings {
    public static final String SPARSE_INDEX = "index.sparse";
    public static final String MEMORY_USAGE = "index.sparse.memory";

    private static SparseSettings INSTANCE;

    public static synchronized SparseSettings state() {
        if (INSTANCE == null) {
            INSTANCE = new SparseSettings();
        }
        return INSTANCE;
    }

    /**
     * This setting identifies sparse index.
     */
    public static final Setting<Boolean> IS_SPARSE_INDEX_SETTING = Setting.boolSetting(SPARSE_INDEX, false, IndexScope, Final);
    /**
     * This is just a fake setting for POC whenever it changes value, we output the memory usage of the in-memory data
     */
    public static final Setting<Boolean> SPARSE_MEMORY_SETTING = Setting.boolSetting(
        MEMORY_USAGE,
        false,
        IndexScope,
        Setting.Property.Dynamic
    );

}
