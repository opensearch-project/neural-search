/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.util;

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

public class TextEmbeddingModel {
    private static TextEmbeddingModel instance = null;

    public String modelId = null;

    private TextEmbeddingModel() {

    }

    public void setModelId(String modelId) {
        this.modelId = modelId;
    }

    public String getModelId() {
        return modelId;
    }

    public static TextEmbeddingModel getInstance() {
        // To ensure only one instance is created
        if (instance == null) {
            instance = new TextEmbeddingModel();
        }
        return instance;
    }
}
