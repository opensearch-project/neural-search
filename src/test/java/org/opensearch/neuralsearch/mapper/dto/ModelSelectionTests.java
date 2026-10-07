/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.mapper.dto;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.mapper.MapperParsingException;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.LANGUAGE_OPTION;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.MODEL_SELECTION;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.MODEL_TYPE;
import static org.opensearch.neuralsearch.util.TestUtils.xContentBuilderToMap;

public class ModelSelectionTests extends OpenSearchTestCase {

    public void testConstruct_fromMap_withBothFields() {
        final ModelSelection ms = new ModelSelection(MODEL_SELECTION, Map.of(LANGUAGE_OPTION, "ENGLISH", MODEL_TYPE, "SPARSE"));
        assertEquals("ENGLISH", ms.getLanguageOption());
        assertEquals("SPARSE", ms.getModelType());
    }

    public void testConstruct_fromMap_normalizesCase() {
        final ModelSelection ms = new ModelSelection(MODEL_SELECTION, Map.of(LANGUAGE_OPTION, "multilingual", MODEL_TYPE, "dense"));
        assertEquals("MULTILINGUAL", ms.getLanguageOption());
        assertEquals("DENSE", ms.getModelType());
    }

    public void testConstruct_fromMap_appliesDefaults() {
        final ModelSelection ms = new ModelSelection(MODEL_SELECTION, new HashMap<>());
        assertEquals("ENGLISH", ms.getLanguageOption());
        assertEquals("SPARSE", ms.getModelType());
    }

    public void testConstruct_fromLanguageAndType() {
        final ModelSelection ms = new ModelSelection("MULTILINGUAL", "DENSE");
        assertEquals("MULTILINGUAL", ms.getLanguageOption());
        assertEquals("DENSE", ms.getModelType());
    }

    public void testConstruct_fromLanguageAndType_appliesDefaultsForNull() {
        final ModelSelection ms = new ModelSelection(null, null);
        assertEquals("ENGLISH", ms.getLanguageOption());
        assertEquals("SPARSE", ms.getModelType());
    }

    public void testConstruct_whenValueNotMap_thenThrow() {
        expectThrows(MapperParsingException.class, () -> new ModelSelection(MODEL_SELECTION, "not_a_map"));
    }

    public void testConstruct_whenInvalidLanguageOption_thenThrow() {
        expectThrows(MapperParsingException.class, () -> new ModelSelection(MODEL_SELECTION, Map.of(LANGUAGE_OPTION, "KLINGON")));
    }

    public void testConstruct_whenInvalidModelType_thenThrow() {
        expectThrows(MapperParsingException.class, () -> new ModelSelection(MODEL_SELECTION, Map.of(MODEL_TYPE, "HYBRID")));
    }

    public void testConstruct_whenUnsupportedKey_thenThrow() {
        expectThrows(MapperParsingException.class, () -> new ModelSelection(MODEL_SELECTION, Map.of("unexpected", "value")));
    }

    public void testIsDense() {
        assertTrue(new ModelSelection("ENGLISH", "DENSE").isDense());
        assertFalse(new ModelSelection("ENGLISH", "SPARSE").isDense());
    }

    public void testGetProfileKey() {
        assertEquals("sparse.english", new ModelSelection("ENGLISH", "SPARSE").getProfileKey());
        assertEquals("dense.multilingual", new ModelSelection("MULTILINGUAL", "DENSE").getProfileKey());
    }

    public void testToXContent() throws Exception {
        final ModelSelection ms = new ModelSelection("MULTILINGUAL", "DENSE");
        final XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        ms.toXContent(builder, MODEL_SELECTION);
        builder.endObject();

        @SuppressWarnings("unchecked")
        final Map<String, Object> out = (Map<String, Object>) xContentBuilderToMap(builder).get(MODEL_SELECTION);
        assertEquals("MULTILINGUAL", out.get(LANGUAGE_OPTION));
        assertEquals("DENSE", out.get(MODEL_TYPE));
    }

    public void testToString() {
        final String s = new ModelSelection("ENGLISH", "SPARSE").toString();
        assertTrue(s.contains(LANGUAGE_OPTION));
        assertTrue(s.contains("ENGLISH"));
        assertTrue(s.contains(MODEL_TYPE));
        assertTrue(s.contains("SPARSE"));
    }

    public void testEqualsAndHashCode() {
        final ModelSelection a = new ModelSelection("ENGLISH", "SPARSE");
        final ModelSelection b = new ModelSelection("english", "sparse");
        final ModelSelection c = new ModelSelection("MULTILINGUAL", "SPARSE");
        final ModelSelection d = new ModelSelection("ENGLISH", "DENSE");

        assertEquals(a, a);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
        assertNotEquals(a, d);
        assertNotEquals(a, null);
        assertNotEquals(a, "ENGLISH");
    }
}
