/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.plugin.query;

import static org.opensearch.index.query.AbstractQueryBuilder.BOOST_FIELD;
import static org.opensearch.index.query.AbstractQueryBuilder.NAME_FIELD;
import static org.opensearch.neuralsearch.plugin.TestUtils.xContentBuilderToMap;
import static org.opensearch.neuralsearch.plugin.query.NeuralQueryBuilder.K_FIELD;
import static org.opensearch.neuralsearch.plugin.query.NeuralQueryBuilder.MODEL_ID_FIELD;
import static org.opensearch.neuralsearch.plugin.query.NeuralQueryBuilder.NAME;
import static org.opensearch.neuralsearch.plugin.query.NeuralQueryBuilder.QUERY_TEXT_FIELD;

import java.io.IOException;
import java.util.Map;

import lombok.SneakyThrows;

import org.opensearch.common.ParsingException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

public class NeuralQueryBuilderTests extends OpenSearchTestCase {

    private static final String FIELD_NAME = "testField";
    private static final String QUERY_TEXT = "Hello world!";
    private static final String MODEL_ID = "mfgfgdsfgfdgsde";
    private static final int K = 10;
    private static final float BOOST = 1.8f;
    private static final String QUERY_NAME = "queryName";

    @SneakyThrows
    public void testFromXContent_whenBuiltWithDefaults_thenBuildSuccessfully() {
        /*
          {
              "VECTOR_FIELD": {
                "query_text": "string",
                "model_id": "string",
                "k": int
              }
          }
        */
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(FIELD_NAME)
            .field(QUERY_TEXT_FIELD.getPreferredName(), QUERY_TEXT)
            .field(MODEL_ID_FIELD.getPreferredName(), MODEL_ID)
            .field(K_FIELD.getPreferredName(), K)
            .endObject()
            .endObject();

        XContentParser contentParser = createParser(xContentBuilder);
        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.fromXContent(contentParser);

        assertEquals(FIELD_NAME, neuralQueryBuilder.fieldName());
        assertEquals(QUERY_TEXT, neuralQueryBuilder.queryText());
        assertEquals(MODEL_ID, neuralQueryBuilder.modelId());
        assertEquals(K, neuralQueryBuilder.k());
    }

    @SneakyThrows
    public void testFromXContent_whenBuiltWithOptionals_thenBuildSuccessfully() {
        /*
          {
              "VECTOR_FIELD": {
                "query_text": "string",
                "model_id": "string",
                "k": int,
                "boost": 10.0,
                "_name": "something",
              }
          }
        */
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(FIELD_NAME)
            .field(QUERY_TEXT_FIELD.getPreferredName(), QUERY_TEXT)
            .field(MODEL_ID_FIELD.getPreferredName(), MODEL_ID)
            .field(K_FIELD.getPreferredName(), K)
            .field(BOOST_FIELD.getPreferredName(), BOOST)
            .field(NAME_FIELD.getPreferredName(), QUERY_NAME)
            .endObject()
            .endObject();

        XContentParser contentParser = createParser(xContentBuilder);
        NeuralQueryBuilder neuralQueryBuilder = NeuralQueryBuilder.fromXContent(contentParser);

        assertEquals(FIELD_NAME, neuralQueryBuilder.fieldName());
        assertEquals(QUERY_TEXT, neuralQueryBuilder.queryText());
        assertEquals(MODEL_ID, neuralQueryBuilder.modelId());
        assertEquals(K, neuralQueryBuilder.k());
        assertEquals(BOOST, neuralQueryBuilder.boost(), 0.0);
        assertEquals(QUERY_NAME, neuralQueryBuilder.queryName());
    }

    @SneakyThrows
    public void testFromXContent_whenBuildWithMultipleRootFields_thenFail() {
        /*
          {
              "VECTOR_FIELD": {
                "query_text": "string",
                "model_id": "string",
                "k": int,
                "boost": 10.0,
                "_name": "something",
              },
              "invalid": 10
          }
        */
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(FIELD_NAME)
            .field(QUERY_TEXT_FIELD.getPreferredName(), QUERY_TEXT)
            .field(MODEL_ID_FIELD.getPreferredName(), MODEL_ID)
            .field(K_FIELD.getPreferredName(), K)
            .field(BOOST_FIELD.getPreferredName(), BOOST)
            .field(NAME_FIELD.getPreferredName(), QUERY_NAME)
            .endObject()
            .field("invalid", 10)
            .endObject();

        XContentParser contentParser = createParser(xContentBuilder);
        expectThrows(ParsingException.class, () -> NeuralQueryBuilder.fromXContent(contentParser));
    }

    @SneakyThrows
    public void testFromXContent_whenBuildWithMissingParameters_thenFail() {
        /*
          {
              "VECTOR_FIELD": {

              }
          }
        */
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject(FIELD_NAME).endObject().endObject();

        XContentParser contentParser = createParser(xContentBuilder);
        expectThrows(IllegalArgumentException.class, () -> NeuralQueryBuilder.fromXContent(contentParser));
    }

    @SneakyThrows
    public void testFromXContent_whenBuildWithDuplicateParameters_thenFail() {
        /*
          {
              "VECTOR_FIELD": {
                "query_text": "string",
                "query_text": "string",
                "model_id": "string",
                "model_id": "string",
                "k": int,
                "k": int
              }
          }
        */
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject(FIELD_NAME)
            .field(QUERY_TEXT_FIELD.getPreferredName(), QUERY_TEXT)
            .field(MODEL_ID_FIELD.getPreferredName(), MODEL_ID)
            .field(K_FIELD.getPreferredName(), K)
            .field(QUERY_TEXT_FIELD.getPreferredName(), QUERY_TEXT)
            .field(MODEL_ID_FIELD.getPreferredName(), MODEL_ID)
            .field(K_FIELD.getPreferredName(), K)
            .endObject()
            .endObject();

        XContentParser contentParser = createParser(xContentBuilder);
        expectThrows(IOException.class, () -> NeuralQueryBuilder.fromXContent(contentParser));
    }

    @SuppressWarnings("unchecked")
    @SneakyThrows
    public void testToXContent() {
        NeuralQueryBuilder neuralQueryBuilder = new NeuralQueryBuilder().fieldName(FIELD_NAME).modelId(MODEL_ID).queryText(QUERY_TEXT).k(K);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder = neuralQueryBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
        Map<String, Object> out = xContentBuilderToMap(builder);

        Object outer = out.get(NAME);
        if (!(outer instanceof Map)) {
            fail("neural does not map to nested object");
        }

        Map<String, Object> outerMap = (Map<String, Object>) outer;

        assertEquals(1, outerMap.size());
        assertTrue(outerMap.containsKey(FIELD_NAME));

        Object secondInner = outerMap.get(FIELD_NAME);
        if (!(secondInner instanceof Map)) {
            fail("field name does not map to nested object");
        }

        Map<String, Object> secondInnerMap = (Map<String, Object>) secondInner;

        assertEquals(MODEL_ID, secondInnerMap.get(MODEL_ID_FIELD.getPreferredName()));
        assertEquals(QUERY_TEXT, secondInnerMap.get(QUERY_TEXT_FIELD.getPreferredName()));
        assertEquals(K, secondInnerMap.get(K_FIELD.getPreferredName()));
    }

    @SneakyThrows
    public void testStreams() {
        NeuralQueryBuilder original = new NeuralQueryBuilder();
        original.fieldName(FIELD_NAME);
        original.queryText(QUERY_TEXT);
        original.modelId(MODEL_ID);
        original.k(K);
        original.boost(BOOST);
        original.queryName(QUERY_NAME);

        BytesStreamOutput streamOutput = new BytesStreamOutput();
        original.writeTo(streamOutput);

        NeuralQueryBuilder copy = new NeuralQueryBuilder(streamOutput.bytes().streamInput());
        assertEquals(original, copy);
    }

    public void testHashAndEquals() {
        String fieldName1 = "field 1";
        String fieldName2 = "field 2";
        String queryText1 = "query text 1";
        String queryText2 = "query text 2";
        String modelId1 = "model-1";
        String modelId2 = "model-2";
        float boost1 = 1.8f;
        float boost2 = 3.8f;
        String queryName1 = "query-1";
        String queryName2 = "query-2";
        int k1 = 1;
        int k2 = 2;

        NeuralQueryBuilder neuralQueryBuilder_baseline = new NeuralQueryBuilder().fieldName(fieldName1)
            .queryText(queryText1)
            .modelId(modelId1)
            .k(k1)
            .boost(boost1)
            .queryName(queryName1);

        // Identical to neuralQueryBuilder_baseline
        NeuralQueryBuilder neuralQueryBuilder_baselineCopy = new NeuralQueryBuilder().fieldName(fieldName1)
            .queryText(queryText1)
            .modelId(modelId1)
            .k(k1)
            .boost(boost1)
            .queryName(queryName1);

        // Identical to neuralQueryBuilder_baseline except default boost and query name
        NeuralQueryBuilder neuralQueryBuilder_defaultBoostAndQueryName = new NeuralQueryBuilder().fieldName(fieldName1)
            .queryText(queryText1)
            .modelId(modelId1)
            .k(k1);

        // Identical to neuralQueryBuilder_baseline except diff field name
        NeuralQueryBuilder neuralQueryBuilder_diffFieldName = new NeuralQueryBuilder().fieldName(fieldName2)
            .queryText(queryText1)
            .modelId(modelId1)
            .k(k1)
            .boost(boost1)
            .queryName(queryName1);

        // Identical to neuralQueryBuilder_baseline except diff query text
        NeuralQueryBuilder neuralQueryBuilder_diffQueryText = new NeuralQueryBuilder().fieldName(fieldName1)
            .queryText(queryText2)
            .modelId(modelId1)
            .k(k1)
            .boost(boost1)
            .queryName(queryName1);

        // Identical to neuralQueryBuilder_baseline except diff model ID
        NeuralQueryBuilder neuralQueryBuilder_diffModelId = new NeuralQueryBuilder().fieldName(fieldName1)
            .queryText(queryText1)
            .modelId(modelId2)
            .k(k1)
            .boost(boost1)
            .queryName(queryName1);

        // Identical to neuralQueryBuilder_baseline except diff k
        NeuralQueryBuilder neuralQueryBuilder_diffK = new NeuralQueryBuilder().fieldName(fieldName1)
            .queryText(queryText1)
            .modelId(modelId1)
            .k(k2)
            .boost(boost1)
            .queryName(queryName1);

        // Identical to neuralQueryBuilder_baseline except diff boost
        NeuralQueryBuilder neuralQueryBuilder_diffBoost = new NeuralQueryBuilder().fieldName(fieldName1)
            .queryText(queryText1)
            .modelId(modelId1)
            .k(k1)
            .boost(boost2)
            .queryName(queryName1);

        // Identical to neuralQueryBuilder_baseline except diff query name
        NeuralQueryBuilder neuralQueryBuilder_diffQueryName = new NeuralQueryBuilder().fieldName(fieldName1)
            .queryText(queryText1)
            .modelId(modelId1)
            .k(k1)
            .boost(boost1)
            .queryName(queryName2);

        assertEquals(neuralQueryBuilder_baseline, neuralQueryBuilder_baseline);
        assertEquals(neuralQueryBuilder_baseline.hashCode(), neuralQueryBuilder_baseline.hashCode());

        assertEquals(neuralQueryBuilder_baseline, neuralQueryBuilder_baselineCopy);
        assertEquals(neuralQueryBuilder_baseline.hashCode(), neuralQueryBuilder_baselineCopy.hashCode());

        assertNotEquals(neuralQueryBuilder_baseline, neuralQueryBuilder_defaultBoostAndQueryName);
        assertNotEquals(neuralQueryBuilder_baseline.hashCode(), neuralQueryBuilder_defaultBoostAndQueryName.hashCode());

        assertNotEquals(neuralQueryBuilder_baseline, neuralQueryBuilder_diffFieldName);
        assertNotEquals(neuralQueryBuilder_baseline.hashCode(), neuralQueryBuilder_diffFieldName.hashCode());

        assertNotEquals(neuralQueryBuilder_baseline, neuralQueryBuilder_diffQueryText);
        assertNotEquals(neuralQueryBuilder_baseline.hashCode(), neuralQueryBuilder_diffQueryText.hashCode());

        assertNotEquals(neuralQueryBuilder_baseline, neuralQueryBuilder_diffModelId);
        assertNotEquals(neuralQueryBuilder_baseline.hashCode(), neuralQueryBuilder_diffModelId.hashCode());

        assertNotEquals(neuralQueryBuilder_baseline, neuralQueryBuilder_diffK);
        assertNotEquals(neuralQueryBuilder_baseline.hashCode(), neuralQueryBuilder_diffK.hashCode());

        assertNotEquals(neuralQueryBuilder_baseline, neuralQueryBuilder_diffBoost);
        assertNotEquals(neuralQueryBuilder_baseline.hashCode(), neuralQueryBuilder_diffBoost.hashCode());

        assertNotEquals(neuralQueryBuilder_baseline, neuralQueryBuilder_diffQueryName);
        assertNotEquals(neuralQueryBuilder_baseline.hashCode(), neuralQueryBuilder_diffQueryName.hashCode());
    }
}
