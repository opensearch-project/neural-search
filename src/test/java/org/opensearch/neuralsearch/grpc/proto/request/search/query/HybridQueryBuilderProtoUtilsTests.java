/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.grpc.proto.request.search.query;

import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.neuralsearch.settings.NeuralSearchSettingsAccessor;
import org.opensearch.neuralsearch.stats.events.EventStatsManager;
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;
import org.opensearch.transport.grpc.spi.QueryBuilderProtoConverterRegistry;
import org.opensearch.protobufs.FieldValue;
import org.opensearch.protobufs.HybridQuery;
import org.opensearch.protobufs.MatchAllQuery;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.protobufs.TermQuery;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.Version;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doReturn;

public class HybridQueryBuilderProtoUtilsTests extends OpenSearchTestCase {

    @Mock
    private QueryBuilderProtoConverterRegistry mockRegistry;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);

        // Mock cluster service for NeuralSearchClusterUtil
        ClusterService mockClusterService = mock(ClusterService.class);
        ClusterState mockClusterState = mock(ClusterState.class);
        Metadata mockMetadata = mock(Metadata.class);
        DiscoveryNodes mockDiscoveryNodes = mock(DiscoveryNodes.class);
        when(mockClusterService.state()).thenReturn(mockClusterState);
        when(mockClusterState.metadata()).thenReturn(mockMetadata);
        when(mockClusterState.getNodes()).thenReturn(mockDiscoveryNodes);
        when(mockDiscoveryNodes.getMinNodeVersion()).thenReturn(Version.CURRENT);
        NeuralSearchClusterUtil.instance().initialize(mockClusterService, null);

        // Initialize EventStatsManager with mock settings accessor
        NeuralSearchSettingsAccessor mockSettingsAccessor = mock(NeuralSearchSettingsAccessor.class);
        when(mockSettingsAccessor.isStatsEnabled()).thenReturn(true);
        EventStatsManager.instance().initialize(mockSettingsAccessor);

        // Set up mock registry to return appropriate query builders
        when(mockRegistry.fromProto(any(QueryContainer.class))).thenAnswer(invocation -> {
            QueryContainer container = invocation.getArgument(0);
            if (container.hasMatchAll()) {
                return new MatchAllQueryBuilder();
            } else if (container.hasTerm()) {
                TermQuery termQuery = container.getTerm();
                return new TermQueryBuilder(termQuery.getField(), termQuery.getValue().getString());
            }
            return null;
        });
    }

    public void testFromProto_withAllFields() {
        HybridQuery hybridQuery = HybridQuery.newBuilder()
            .setXName("test_hybrid_query")
            .addQueries(createTermQueryContainer("field1", "value1"))
            .addQueries(createTermQueryContainer("field2", "value2"))
            .addQueries(createMatchAllQueryContainer())
            .setPaginationDepth(100)
            .setFilter(createTermQueryContainer("filter_field", "filter_value"))
            .build();

        QueryBuilder result = HybridQueryBuilderProtoUtils.fromProto(hybridQuery, mockRegistry);

        assertNotNull(result);
        assertTrue(result instanceof HybridQueryBuilder);
        HybridQueryBuilder hybridQueryBuilder = (HybridQueryBuilder) result;

        assertEquals("test_hybrid_query", hybridQueryBuilder.queryName());
        assertEquals(3, hybridQueryBuilder.queries().size());
        assertEquals(Integer.valueOf(100), hybridQueryBuilder.paginationDepth());
    }

    public void testFromProto_withMinimalFields() {
        HybridQuery hybridQuery = HybridQuery.newBuilder().addQueries(createTermQueryContainer("field1", "value1")).build();

        QueryBuilder result = HybridQueryBuilderProtoUtils.fromProto(hybridQuery, mockRegistry);

        assertNotNull(result);
        assertTrue(result instanceof HybridQueryBuilder);
        HybridQueryBuilder hybridQueryBuilder = (HybridQueryBuilder) result;

        assertNull(hybridQueryBuilder.queryName());
        assertEquals(1.0f, hybridQueryBuilder.boost(), 0.001f);
        assertEquals(1, hybridQueryBuilder.queries().size());
        assertNull(hybridQueryBuilder.paginationDepth());
    }

    public void testFromProto_withQueryName() {
        HybridQuery hybridQuery = HybridQuery.newBuilder()
            .setXName("my_hybrid_query")
            .addQueries(createTermQueryContainer("field1", "value1"))
            .build();

        QueryBuilder result = HybridQueryBuilderProtoUtils.fromProto(hybridQuery, mockRegistry);

        assertNotNull(result);
        assertTrue(result instanceof HybridQueryBuilder);
        HybridQueryBuilder hybridQueryBuilder = (HybridQueryBuilder) result;

        assertEquals("my_hybrid_query", hybridQueryBuilder.queryName());
    }

    public void testFromProto_withMultipleQueries() {
        HybridQuery hybridQuery = HybridQuery.newBuilder()
            .addQueries(createTermQueryContainer("field1", "value1"))
            .addQueries(createTermQueryContainer("field2", "value2"))
            .addQueries(createMatchAllQueryContainer())
            .addQueries(createTermQueryContainer("field3", "value3"))
            .build();

        QueryBuilder result = HybridQueryBuilderProtoUtils.fromProto(hybridQuery, mockRegistry);

        assertNotNull(result);
        assertTrue(result instanceof HybridQueryBuilder);
        HybridQueryBuilder hybridQueryBuilder = (HybridQueryBuilder) result;

        assertEquals(4, hybridQueryBuilder.queries().size());
    }

    public void testFromProto_withPaginationDepth() {
        HybridQuery hybridQuery = HybridQuery.newBuilder()
            .addQueries(createTermQueryContainer("field1", "value1"))
            .setPaginationDepth(50)
            .build();

        QueryBuilder result = HybridQueryBuilderProtoUtils.fromProto(hybridQuery, mockRegistry);

        assertNotNull(result);
        assertTrue(result instanceof HybridQueryBuilder);
        HybridQueryBuilder hybridQueryBuilder = (HybridQueryBuilder) result;

        assertEquals(Integer.valueOf(50), hybridQueryBuilder.paginationDepth());
    }

    public void testFromProto_withFilter() {
        HybridQuery hybridQuery = HybridQuery.newBuilder()
            .addQueries(createTermQueryContainer("field1", "value1"))
            .addQueries(createTermQueryContainer("field2", "value2"))
            .setFilter(createTermQueryContainer("filter_field", "filter_value"))
            .build();

        QueryBuilder result = HybridQueryBuilderProtoUtils.fromProto(hybridQuery, mockRegistry);

        assertNotNull(result);
        assertTrue(result instanceof HybridQueryBuilder);
        HybridQueryBuilder hybridQueryBuilder = (HybridQueryBuilder) result;

        // Filter should be applied to all sub-queries
        assertEquals(2, hybridQueryBuilder.queries().size());
    }

    public void testFromProto_exceedsMaxQueries() {
        HybridQuery.Builder builder = HybridQuery.newBuilder();

        // Add more than MAX_NUMBER_OF_SUB_QUERIES (5)
        for (int i = 0; i < HybridQueryBuilder.MAX_NUMBER_OF_SUB_QUERIES + 1; i++) {
            builder.addQueries(createTermQueryContainer("field" + i, "value" + i));
        }

        HybridQuery hybridQuery = builder.build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> HybridQueryBuilderProtoUtils.fromProto(hybridQuery, mockRegistry)
        );

        assertTrue(exception.getMessage().contains("Number of sub-queries exceeds maximum"));
    }

    public void testFromProto_noQueries() {
        HybridQuery hybridQuery = HybridQuery.newBuilder().build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> HybridQueryBuilderProtoUtils.fromProto(hybridQuery, mockRegistry)
        );

        assertTrue(exception.getMessage().contains("requires 'queries' field with at least one clause"));
    }

    public void testFromProto_withNonDefaultBoost() {
        HybridQuery hybridQuery = HybridQuery.newBuilder().addQueries(createTermQueryContainer("field1", "value1")).setBoost(2.0f).build();

        // HybridQueryBuilder does not support non-default boost values (must be 1.0)
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> HybridQueryBuilderProtoUtils.fromProto(hybridQuery, mockRegistry)
        );

        assertTrue(exception.getMessage().contains("does not support"));
        assertTrue(exception.getMessage().contains("boost"));
    }

    public void testFromProto_withDefaultBoost() {
        HybridQuery hybridQuery = HybridQuery.newBuilder().addQueries(createTermQueryContainer("field1", "value1")).setBoost(1.0f).build();

        QueryBuilder result = HybridQueryBuilderProtoUtils.fromProto(hybridQuery, mockRegistry);

        assertNotNull(result);
        assertTrue(result instanceof HybridQueryBuilder);
        HybridQueryBuilder hybridQueryBuilder = (HybridQueryBuilder) result;

        assertEquals(1.0f, hybridQueryBuilder.boost(), 0.001f);
    }

    public void testFromProto_withNullQueryBuilder() {
        // Set up mock to return null for one query
        when(mockRegistry.fromProto(any(QueryContainer.class))).thenReturn(null);

        HybridQuery hybridQuery = HybridQuery.newBuilder()
            .addQueries(createTermQueryContainer("field1", "value1"))
            .build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> HybridQueryBuilderProtoUtils.fromProto(hybridQuery, mockRegistry)
        );

        assertTrue(exception.getMessage().contains("requires 'queries' field with at least one clause"));
    }

    public void testFromProto_withNullFilter() {
        // Override mock to return null for MatchAll queries (used as filter)
        doReturn(null).when(mockRegistry).fromProto(createMatchAllQueryContainer());

        HybridQuery hybridQuery = HybridQuery.newBuilder()
            .addQueries(createTermQueryContainer("field1", "value1"))
            .setFilter(createMatchAllQueryContainer())
            .build();

        // Should succeed - null filter should be ignored gracefully
        QueryBuilder result = HybridQueryBuilderProtoUtils.fromProto(hybridQuery, mockRegistry);

        assertNotNull(result);
        assertTrue(result instanceof HybridQueryBuilder);
        HybridQueryBuilder hybridQueryBuilder = (HybridQueryBuilder) result;
        assertEquals(1, hybridQueryBuilder.queries().size());

        // Assert filter was not applied: sub-query should still be TermQueryBuilder (not wrapped in BoolQueryBuilder)
        QueryBuilder subQuery = hybridQueryBuilder.queries().get(0);
        assertTrue("Sub-query should be TermQueryBuilder when filter is null", subQuery instanceof TermQueryBuilder);

        // Compare with control case without filter to ensure they're equivalent
        HybridQuery controlQuery = HybridQuery.newBuilder().addQueries(createTermQueryContainer("field1", "value1")).build();
        QueryBuilder controlResult = HybridQueryBuilderProtoUtils.fromProto(controlQuery, mockRegistry);
        HybridQueryBuilder controlBuilder = (HybridQueryBuilder) controlResult;
        assertEquals("Queries should be identical when filter is null", controlBuilder.queries().get(0).getClass(), subQuery.getClass());
    }

    private QueryContainer createTermQueryContainer(String field, String value) {
        return QueryContainer.newBuilder()
            .setTerm(TermQuery.newBuilder().setField(field).setValue(FieldValue.newBuilder().setString(value).build()).build())
            .build();
    }

    private QueryContainer createMatchAllQueryContainer() {
        return QueryContainer.newBuilder().setMatchAll(MatchAllQuery.newBuilder().build()).build();
    }
}
