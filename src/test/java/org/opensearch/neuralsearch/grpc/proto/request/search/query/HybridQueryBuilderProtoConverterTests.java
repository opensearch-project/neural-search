/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.grpc.proto.request.search.query;

import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.Version;
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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HybridQueryBuilderProtoConverterTests extends OpenSearchTestCase {

    private HybridQueryBuilderProtoConverter converter;

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

        converter = new HybridQueryBuilderProtoConverter();
        converter.setRegistry(mockRegistry);

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

    public void testGetHandledQueryCase() {
        assertEquals(QueryContainer.QueryContainerCase.HYBRID, converter.getHandledQueryCase());
    }

    public void testFromProto_validHybridQuery() {
        HybridQuery hybridQuery = HybridQuery.newBuilder()
            .setXName("test_hybrid_query")
            .setBoost(1.0f)
            .addQueries(createTermQueryContainer("field1", "value1"))
            .addQueries(createTermQueryContainer("field2", "value2"))
            .build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setHybrid(hybridQuery).build();

        QueryBuilder result = converter.fromProto(queryContainer);

        assertNotNull(result);
        assertTrue(result instanceof HybridQueryBuilder);
        HybridQueryBuilder hybridQueryBuilder = (HybridQueryBuilder) result;
        assertEquals("test_hybrid_query", hybridQueryBuilder.queryName());
        assertEquals(2, hybridQueryBuilder.queries().size());
    }

    public void testFromProto_minimalHybridQuery() {
        HybridQuery hybridQuery = HybridQuery.newBuilder().addQueries(createTermQueryContainer("field1", "value1")).build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setHybrid(hybridQuery).build();

        QueryBuilder result = converter.fromProto(queryContainer);

        assertNotNull(result);
        assertTrue(result instanceof HybridQueryBuilder);
        HybridQueryBuilder hybridQueryBuilder = (HybridQueryBuilder) result;
        assertNull(hybridQueryBuilder.queryName());
        assertEquals(1.0f, hybridQueryBuilder.boost(), 0.001f);
        assertEquals(1, hybridQueryBuilder.queries().size());
    }

    public void testFromProto_withNullQueryContainer() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(null));

        assertTrue(exception.getMessage().contains("QueryContainer does not contain a Hybrid query"));
    }

    public void testFromProto_withoutHybridQuery() {
        QueryContainer queryContainer = QueryContainer.newBuilder().build();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(queryContainer));

        assertTrue(exception.getMessage().contains("QueryContainer does not contain a Hybrid query"));
    }

    public void testFromProto_withDifferentQueryType() {
        QueryContainer queryContainer = QueryContainer.newBuilder()
            .setBool(org.opensearch.protobufs.BoolQuery.newBuilder().build())
            .build();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(queryContainer));

        assertTrue(exception.getMessage().contains("QueryContainer does not contain a Hybrid query"));
    }

    public void testFromProto_complexHybridQuery() {
        HybridQuery.Builder hybridProtoBuilder = HybridQuery.newBuilder().setXName("complex_hybrid_query").setPaginationDepth(100);

        for (int i = 0; i < 5; i++) {
            hybridProtoBuilder.addQueries(createTermQueryContainer("field_" + i, "value_" + i));
        }

        HybridQuery hybridQuery = hybridProtoBuilder.build();
        QueryContainer queryContainer = QueryContainer.newBuilder().setHybrid(hybridQuery).build();

        QueryBuilder result = converter.fromProto(queryContainer);

        assertNotNull(result);
        assertTrue(result instanceof HybridQueryBuilder);
        HybridQueryBuilder hybridQueryBuilder = (HybridQueryBuilder) result;
        assertEquals("complex_hybrid_query", hybridQueryBuilder.queryName());
        assertEquals(5, hybridQueryBuilder.queries().size());
        assertEquals(Integer.valueOf(100), hybridQueryBuilder.paginationDepth());
    }

    public void testFromProto_withFilter() {
        HybridQuery hybridQuery = HybridQuery.newBuilder()
            .addQueries(createTermQueryContainer("field1", "value1"))
            .setFilter(createTermQueryContainer("filter_field", "filter_value"))
            .build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setHybrid(hybridQuery).build();

        QueryBuilder result = converter.fromProto(queryContainer);

        assertNotNull(result);
        assertTrue(result instanceof HybridQueryBuilder);
        HybridQueryBuilder hybridQueryBuilder = (HybridQueryBuilder) result;
        assertEquals(1, hybridQueryBuilder.queries().size());
    }

    public void testSetRegistry() {
        HybridQueryBuilderProtoConverter newConverter = new HybridQueryBuilderProtoConverter();
        assertNotNull(newConverter);

        newConverter.setRegistry(mockRegistry);

        HybridQuery hybridQuery = HybridQuery.newBuilder().addQueries(createTermQueryContainer("field1", "value1")).build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setHybrid(hybridQuery).build();
        QueryBuilder result = newConverter.fromProto(queryContainer);

        assertNotNull(result);
        assertTrue(result instanceof HybridQueryBuilder);
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
