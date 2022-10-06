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

    public void testFromXContent_valid_withDefaults() throws IOException {
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

        assertEquals(FIELD_NAME, neuralQueryBuilder.getFieldName());
        assertEquals(QUERY_TEXT, neuralQueryBuilder.getQueryText());
        assertEquals(MODEL_ID, neuralQueryBuilder.getModelId());
        assertEquals(K, neuralQueryBuilder.getK());
    }

    public void testFromXContent_valid_withOptionals() throws IOException {
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

        assertEquals(FIELD_NAME, neuralQueryBuilder.getFieldName());
        assertEquals(QUERY_TEXT, neuralQueryBuilder.getQueryText());
        assertEquals(MODEL_ID, neuralQueryBuilder.getModelId());
        assertEquals(K, neuralQueryBuilder.getK());
        assertEquals(BOOST, neuralQueryBuilder.boost(), 0.0);
        assertEquals(QUERY_NAME, neuralQueryBuilder.queryName());
    }

    public void testFromXContent_invalid_multipleRootFields() throws IOException {
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

    public void testFromXContent_invalid_missingParameters() throws IOException {
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

    public void testFromXContent_invalid_duplicateParameters() throws IOException {
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
    public void testToXContent() throws IOException {
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

    public void testStreams() throws IOException {
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

        NeuralQueryBuilder neuralQueryBuilder1 = new NeuralQueryBuilder().fieldName(fieldName1)
            .queryText(queryText1)
            .modelId(modelId1)
            .k(k1)
            .boost(boost1)
            .queryName(queryName1);

        // Identical to neuralQueryBuilder1
        NeuralQueryBuilder neuralQueryBuilder2 = new NeuralQueryBuilder().fieldName(fieldName1)
            .queryText(queryText1)
            .modelId(modelId1)
            .k(k1)
            .boost(boost1)
            .queryName(queryName1);

        // Identical to neuralQueryBuilder1 except default boost and query name
        NeuralQueryBuilder neuralQueryBuilder3 = new NeuralQueryBuilder().fieldName(fieldName1)
            .queryText(queryText1)
            .modelId(modelId1)
            .k(k1);

        // Identical to neuralQueryBuilder1 except diff field name
        NeuralQueryBuilder neuralQueryBuilder4 = new NeuralQueryBuilder().fieldName(fieldName2)
            .queryText(queryText1)
            .modelId(modelId1)
            .k(k1)
            .boost(boost1)
            .queryName(queryName1);

        // Identical to neuralQueryBuilder1 except diff query text
        NeuralQueryBuilder neuralQueryBuilder5 = new NeuralQueryBuilder().fieldName(fieldName1)
            .queryText(queryText2)
            .modelId(modelId1)
            .k(k1)
            .boost(boost1)
            .queryName(queryName1);

        // Identical to neuralQueryBuilder1 except diff model ID
        NeuralQueryBuilder neuralQueryBuilder6 = new NeuralQueryBuilder().fieldName(fieldName1)
            .queryText(queryText1)
            .modelId(modelId2)
            .k(k1)
            .boost(boost1)
            .queryName(queryName1);

        // Identical to neuralQueryBuilder1 except diff k
        NeuralQueryBuilder neuralQueryBuilder7 = new NeuralQueryBuilder().fieldName(fieldName1)
            .queryText(queryText1)
            .modelId(modelId1)
            .k(k2)
            .boost(boost1)
            .queryName(queryName1);

        // Identical to neuralQueryBuilder1 except diff boost
        NeuralQueryBuilder neuralQueryBuilder8 = new NeuralQueryBuilder().fieldName(fieldName1)
            .queryText(queryText1)
            .modelId(modelId1)
            .k(k1)
            .boost(boost2)
            .queryName(queryName1);

        // Identical to neuralQueryBuilder1 except diff query name
        NeuralQueryBuilder neuralQueryBuilder9 = new NeuralQueryBuilder().fieldName(fieldName1)
            .queryText(queryText1)
            .modelId(modelId1)
            .k(k1)
            .boost(boost1)
            .queryName(queryName2);

        assertEquals(neuralQueryBuilder1, neuralQueryBuilder1);
        assertEquals(neuralQueryBuilder1.hashCode(), neuralQueryBuilder1.hashCode());

        assertEquals(neuralQueryBuilder1, neuralQueryBuilder2);
        assertEquals(neuralQueryBuilder1.hashCode(), neuralQueryBuilder2.hashCode());

        assertNotEquals(neuralQueryBuilder1, neuralQueryBuilder3);
        assertNotEquals(neuralQueryBuilder1.hashCode(), neuralQueryBuilder3.hashCode());

        assertNotEquals(neuralQueryBuilder1, neuralQueryBuilder4);
        assertNotEquals(neuralQueryBuilder1.hashCode(), neuralQueryBuilder4.hashCode());

        assertNotEquals(neuralQueryBuilder1, neuralQueryBuilder5);
        assertNotEquals(neuralQueryBuilder1.hashCode(), neuralQueryBuilder5.hashCode());

        assertNotEquals(neuralQueryBuilder1, neuralQueryBuilder6);
        assertNotEquals(neuralQueryBuilder1.hashCode(), neuralQueryBuilder6.hashCode());

        assertNotEquals(neuralQueryBuilder1, neuralQueryBuilder7);
        assertNotEquals(neuralQueryBuilder1.hashCode(), neuralQueryBuilder7.hashCode());

        assertNotEquals(neuralQueryBuilder1, neuralQueryBuilder8);
        assertNotEquals(neuralQueryBuilder1.hashCode(), neuralQueryBuilder8.hashCode());

        assertNotEquals(neuralQueryBuilder1, neuralQueryBuilder9);
        assertNotEquals(neuralQueryBuilder1.hashCode(), neuralQueryBuilder9.hashCode());
    }
}
