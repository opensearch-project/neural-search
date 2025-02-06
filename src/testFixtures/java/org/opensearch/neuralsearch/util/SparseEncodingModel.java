/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.util;

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

public class SparseEncodingModel {
    private static SparseEncodingModel instance = null;

    public String modelId = null;

    private SparseEncodingModel() {

    }

    public void setModelId(String modelId) {
        this.modelId = modelId;
    }

    public String getModelId() {
        return modelId;
    }

    public static SparseEncodingModel getInstance() {
        // To ensure only one instance is created
        if (instance == null) {
            instance = new SparseEncodingModel();
        }
        return instance;
    }
}
