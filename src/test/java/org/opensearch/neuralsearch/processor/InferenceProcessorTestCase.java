/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.math.RandomUtils;
import org.opensearch.index.mapper.IndexFieldMapper;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.IngestDocumentWrapper;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InferenceProcessorTestCase extends OpenSearchTestCase {

    protected List<IngestDocumentWrapper> createIngestDocumentWrappers(int count) {
        List<IngestDocumentWrapper> wrapperList = new ArrayList<>();
        for (int i = 0; i < count; ++i) {
            Map<String, Object> sourceAndMetadata = new HashMap<>();
            sourceAndMetadata.put("key1", "value1");
            sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
            wrapperList.add(new IngestDocumentWrapper(i, new IngestDocument(sourceAndMetadata, new HashMap<>()), null));
        }
        return wrapperList;
    }

    protected List<List<Float>> createMockVectorWithLength(int size) {
        float suffix = .234f;
        List<List<Float>> result = new ArrayList<>();
        for (int i = 0; i < size * 2;) {
            List<Float> number = new ArrayList<>();
            number.add(i++ + suffix);
            number.add(i++ + suffix);
            result.add(number);
        }
        return result;
    }

    protected List<List<Float>> createMockVectorResult() {
        List<List<Float>> modelTensorList = new ArrayList<>();
        List<Float> number1 = ImmutableList.of(1.234f, 2.354f);
        List<Float> number2 = ImmutableList.of(3.234f, 4.354f);
        List<Float> number3 = ImmutableList.of(5.234f, 6.354f);
        List<Float> number4 = ImmutableList.of(7.234f, 8.354f);
        List<Float> number5 = ImmutableList.of(9.234f, 10.354f);
        List<Float> number6 = ImmutableList.of(11.234f, 12.354f);
        List<Float> number7 = ImmutableList.of(13.234f, 14.354f);
        modelTensorList.add(number1);
        modelTensorList.add(number2);
        modelTensorList.add(number3);
        modelTensorList.add(number4);
        modelTensorList.add(number5);
        modelTensorList.add(number6);
        modelTensorList.add(number7);
        return modelTensorList;
    }

    protected List<List<Float>> createRandomOneDimensionalMockVector(int numOfVectors, int vectorDimension, float min, float max) {
        List<List<Float>> result = new ArrayList<>();
        for (int i = 0; i < numOfVectors; i++) {
            List<Float> numbers = new ArrayList<>();
            for (int j = 0; j < vectorDimension; j++) {
                Float nextFloat = RandomUtils.nextFloat() * (max - min) + min;
                numbers.add(nextFloat);
            }
            result.add(numbers);
        }
        return result;
    }

    protected Map<String, Object> deepCopy(Map<String, Object> original) {
        Map<String, Object> copy = new HashMap<>();
        for (Map.Entry<String, Object> entry : original.entrySet()) {
            copy.put(entry.getKey(), deepCopyValue(entry.getValue()));
        }
        return copy;
    }

    protected Object deepCopyValue(Object value) {
        if (value instanceof Map<?, ?>) {
            Map<String, Object> newMap = new HashMap<>();
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                newMap.put((String) entry.getKey(), deepCopyValue(entry.getValue()));
            }
            return newMap;
        } else if (value instanceof List<?>) {
            List<Object> newList = new ArrayList<>();
            for (Object item : (List<?>) value) {
                newList.add(deepCopyValue(item));
            }
            return newList;
        } else if (value instanceof String) {
            return new String((String) value);
        } else {
            return value;
        }
    }
}
