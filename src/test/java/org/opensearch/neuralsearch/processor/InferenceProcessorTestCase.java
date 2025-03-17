/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.math.RandomUtils;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.get.MultiGetItemResponse;
import org.opensearch.action.get.MultiGetResponse;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.get.GetResult;
import org.opensearch.index.mapper.IndexFieldMapper;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.IngestDocumentWrapper;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InferenceProcessorTestCase extends OpenSearchTestCase {
    protected List<IngestDocumentWrapper> createIngestDocumentWrappers(int count, String value) {
        List<IngestDocumentWrapper> wrapperList = new ArrayList<>();
        for (int i = 1; i <= count; ++i) {
            Map<String, Object> sourceAndMetadata = new HashMap<>();
            sourceAndMetadata.put("key1", value);
            sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
            sourceAndMetadata.put("_id", String.valueOf(i));
            wrapperList.add(new IngestDocumentWrapper(i, new IngestDocument(sourceAndMetadata, new HashMap<>()), null));
        }
        return wrapperList;
    }

    protected List<IngestDocumentWrapper> createIngestDocumentWrappers(int count) {
        return createIngestDocumentWrappers(count, "value1");
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

    protected GetResponse mockEmptyGetResponse() throws IOException {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .field("_index", "my_index")
            .field("_id", "1")
            .field("found", false)
            .endObject();

        XContentParser contentParser = createParser(xContentBuilder);
        return GetResponse.fromXContent(contentParser);
    }

    protected MultiGetResponse mockEmptyMultiGetItemResponse() throws IOException {
        return new MultiGetResponse(new MultiGetItemResponse[0]);
    }

    protected GetResponse convertToGetResponse(IngestDocument ingestDocument) throws IOException {
        String index = ingestDocument.getSourceAndMetadata().get("_index").toString();
        String id = ingestDocument.getSourceAndMetadata().get("_id").toString();
        Map<String, Object> source = ingestDocument.getSourceAndMetadata();
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.map(source);
        BytesReference bytes = BytesReference.bytes(builder);
        GetResult result = new GetResult(index, id, 0, 1, 1, true, bytes, null, null);
        return new GetResponse(result);
    }

    protected MultiGetResponse convertToMultiGetItemResponse(List<IngestDocumentWrapper> ingestDocuments) throws IOException {
        MultiGetItemResponse[] multiGetItems = new MultiGetItemResponse[ingestDocuments.size()];
        for (int i = 0; i < ingestDocuments.size(); i++) {
            multiGetItems[i] = new MultiGetItemResponse(convertToGetResponse(ingestDocuments.get(i).getIngestDocument()), null);
        }
        return new MultiGetResponse(multiGetItems);
    }
}
