/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.optimization;

import lombok.extern.log4j.Log4j2;
import org.opensearch.ingest.IngestDocument;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * TextImageEmbeddingInferenceFilter optimizes text/image embedding inference by selectively processing text/image data.
 * This class provides efficient text/image embedding processing by comparing text/image between existing and new documents.
 * If both text and image are identical, the corresponding embeddings are copied over, avoiding redundant inference calls and improving performance.
 */
@Log4j2
public class TextImageEmbeddingInferenceFilter {

    public TextImageEmbeddingInferenceFilter() {}

    /**
     * Filters the given knnMap by checking if the values for both text and image are identical in the existing and new document.
     * If both values for text and image match, the corresponding embedding is copied, and empty map is returned, indicating no further
     * processing is required. If any of the two do not match or embedding does not exist, the given knnMap is returned to be processed
     *
     * @return empty Map if embeddings are reused; the original knnMap otherwise.
     */
    public Map<String, String> filterAndCopyExistingEmbeddings(
        IngestDocument ingestDocument,
        Map<String, Object> existingDocument,
        Map<String, String> knnMap,
        String embeddingField
    ) {
        for (Map.Entry<String, String> entry : knnMap.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (existingDocument.containsKey(key) == false || existingDocument.get(key).equals(value) == false) {
                return knnMap;
            }
        }
        Object embeddings = existingDocument.get(embeddingField);
        if (Objects.isNull(embeddings)) {
            return knnMap;
        }
        ingestDocument.setFieldValue(embeddingField, existingDocument.get(embeddingField));
        return Collections.emptyMap();
    }
}
