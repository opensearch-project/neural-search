/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.mapper;

import org.junit.Before;
import org.mockito.MockitoAnnotations;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.search.lookup.SearchLookup;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.APPROXIMATE_THRESHOLD_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.CLUSTER_RATIO_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.NAME_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.N_POSTINGS_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.PARAMETERS_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.SEISMIC;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.SUMMARY_PRUNE_RATIO_FIELD;

public class SparseTokensFieldTypeTests extends AbstractSparseTestBase {
    private SparseTokensFieldType fieldType;
    private SparseMethodContext sparseMethodContext;

    @Before
    @Override
    public void setUp() {
        super.setUp();
        MockitoAnnotations.openMocks(this);

        Map<String, Object> parameters = new HashMap<>();
        parameters.put(SUMMARY_PRUNE_RATIO_FIELD, 0.5f);
        parameters.put(N_POSTINGS_FIELD, 10);
        parameters.put(CLUSTER_RATIO_FIELD, 0.3f);
        parameters.put(APPROXIMATE_THRESHOLD_FIELD, 100);

        Map<String, Object> methodMap = new HashMap<>();
        methodMap.put(NAME_FIELD, SEISMIC);
        methodMap.put(PARAMETERS_FIELD, parameters);
        sparseMethodContext = SparseMethodContext.parse(methodMap);
        fieldType = new SparseTokensFieldType("test_field", sparseMethodContext);
    }

    public void testConstructor_withValidParameters_createsFieldType() {
        assertNotNull(fieldType);
        assertEquals("test_field", fieldType.name());
        assertEquals("sparse_tokens", fieldType.typeName());
        assertEquals(sparseMethodContext, fieldType.getSparseMethodContext());
    }

    public void testValueFetcher_withNullFormat_returnsSourceValueFetcher() {
        QueryShardContext context = mock(QueryShardContext.class);
        SearchLookup searchLookup = mock(SearchLookup.class);

        assertNotNull(fieldType.valueFetcher(context, searchLookup, null));
    }

    public void testValueFetcher_withFormat_throwsException() {
        QueryShardContext context = mock(QueryShardContext.class);
        SearchLookup searchLookup = mock(SearchLookup.class);

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            fieldType.valueFetcher(context, searchLookup, "format");
        });
        assertTrue(exception.getMessage().contains("doesn't support formats"));
    }

    public void testTermQuery_throwsIllegalArgumentException() {
        QueryShardContext context = mock(QueryShardContext.class);

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> { fieldType.termQuery("test_value", context); }
        );
        assertTrue(exception.getMessage().contains("Queries on [sparse_tokens] fields are not supported"));
    }

    public void testExistsQuery_throwsIllegalArgumentException() {
        QueryShardContext context = mock(QueryShardContext.class);

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> { fieldType.existsQuery(context); });
        assertTrue(exception.getMessage().contains("[sparse_tokens] fields do not support [exists] queries"));
    }

    public void testFielddataBuilder_throwsIllegalArgumentException() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            fieldType.fielddataBuilder("test_index", () -> mock(SearchLookup.class));
        });
        assertTrue(exception.getMessage().contains("[sparse_tokens] fields do not support sorting, scripting or aggregating"));
    }

    public void testConstructor_withNullSparseMethodContext_createsFieldType() {
        SparseTokensFieldType nullContextFieldType = new SparseTokensFieldType("null_context_field", null);

        assertNotNull(nullContextFieldType);
        assertEquals("null_context_field", nullContextFieldType.name());
        assertNull(nullContextFieldType.getSparseMethodContext());
    }

    public void testTermQuery_withDifferentValueTypes_throwsException() {
        QueryShardContext context = mock(QueryShardContext.class);

        // Test with different value types
        IllegalArgumentException stringException = expectThrows(IllegalArgumentException.class, () -> {
            fieldType.termQuery("string_value", context);
        });
        assertTrue(stringException.getMessage().contains("Queries on [sparse_tokens] fields are not supported"));

        IllegalArgumentException intException = expectThrows(IllegalArgumentException.class, () -> { fieldType.termQuery(123, context); });
        assertTrue(intException.getMessage().contains("Queries on [sparse_tokens] fields are not supported"));

        IllegalArgumentException nullException = expectThrows(
            IllegalArgumentException.class,
            () -> { fieldType.termQuery(null, context); }
        );
        assertTrue(nullException.getMessage().contains("Queries on [sparse_tokens] fields are not supported"));
    }

    public void testFieldTypeProperties_inheritedFromParent() {
        // Test inherited properties from MappedFieldType
        assertEquals("test_field", fieldType.name());
        assertFalse(fieldType.isSearchable()); // Set to false in constructor
        assertFalse(fieldType.isStored()); // Our stored parameter
        assertFalse(fieldType.hasDocValues()); // Our hasDocValues parameter
    }
}
