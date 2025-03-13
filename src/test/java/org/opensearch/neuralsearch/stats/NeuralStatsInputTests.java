/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.neuralsearch.rest.RestNeuralStatsAction;
import org.opensearch.neuralsearch.stats.events.EventStatName;
import org.opensearch.neuralsearch.stats.info.InfoStatName;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.util.TestUtils.xContentBuilderToMap;

public class NeuralStatsInputTests extends OpenSearchTestCase {
    private static final String NODE_ID_1 = "node1";
    private static final String NODE_ID_2 = "node2";
    private static final EventStatName EVENT_STAT = EventStatName.TEXT_EMBEDDING_PROCESSOR_EXECUTIONS;
    private static final InfoStatName STATE_STAT = InfoStatName.TEXT_EMBEDDING_PROCESSORS;

    public void test_defaultConstructorEmpty() {
        NeuralStatsInput input = new NeuralStatsInput();

        assertTrue(input.getNodeIds().isEmpty());
        assertTrue(input.getEventStatNames().isEmpty());
        assertTrue(input.getInfoStatNames().isEmpty());
        assertFalse(input.isIncludeMetadata());
        assertFalse(input.isFlatten());
    }

    public void test_builderWithAllFields() {
        List<String> nodeIds = Arrays.asList(NODE_ID_1, NODE_ID_2);
        EnumSet<EventStatName> eventStats = EnumSet.of(EVENT_STAT);
        EnumSet<InfoStatName> infoStats = EnumSet.of(STATE_STAT);

        NeuralStatsInput input = NeuralStatsInput.builder()
            .nodeIds(nodeIds)
            .eventStatNames(eventStats)
            .infoStatNames(infoStats)
            .includeMetadata(true)
            .flatten(true)
            .build();

        assertEquals(nodeIds, input.getNodeIds());
        assertEquals(eventStats, input.getEventStatNames());
        assertEquals(infoStats, input.getInfoStatNames());
        assertTrue(input.isIncludeMetadata());
        assertTrue(input.isFlatten());
    }

    public void test_streamInput() throws IOException {
        StreamInput mockInput = mock(StreamInput.class);

        // Have to return the readByte since readBoolean can't be mocked
        when(mockInput.readByte()).thenReturn((byte) 1)   // true for includeMetadata
            .thenReturn((byte) 1);  // true for flatten

        when(mockInput.readOptionalStringList()).thenReturn(Arrays.asList(NODE_ID_1, NODE_ID_2));
        when(mockInput.readOptionalEnumSet(EventStatName.class)).thenReturn(EnumSet.of(EVENT_STAT));
        when(mockInput.readOptionalEnumSet(InfoStatName.class)).thenReturn(EnumSet.of(STATE_STAT));

        NeuralStatsInput input = new NeuralStatsInput(mockInput);

        assertEquals(Arrays.asList(NODE_ID_1, NODE_ID_2), input.getNodeIds());
        assertEquals(EnumSet.of(EVENT_STAT), input.getEventStatNames());
        assertEquals(EnumSet.of(STATE_STAT), input.getInfoStatNames());
        assertTrue(input.isIncludeMetadata());
        assertTrue(input.isFlatten());

        verify(mockInput, times(2)).readByte();
        verify(mockInput, times(1)).readOptionalStringList();
        verify(mockInput, times(2)).readOptionalEnumSet(any());
    }

    public void test_writeToOutputs() throws IOException {
        List<String> nodeIds = Arrays.asList(NODE_ID_1, NODE_ID_2);
        EnumSet<EventStatName> eventStats = EnumSet.of(EVENT_STAT);
        EnumSet<InfoStatName> infoStats = EnumSet.of(STATE_STAT);

        NeuralStatsInput input = NeuralStatsInput.builder()
            .nodeIds(nodeIds)
            .eventStatNames(eventStats)
            .infoStatNames(infoStats)
            .includeMetadata(true)
            .flatten(true)
            .build();

        StreamOutput mockOutput = mock(StreamOutput.class);
        input.writeTo(mockOutput);

        verify(mockOutput).writeOptionalStringCollection(nodeIds);
        verify(mockOutput).writeOptionalEnumSet(eventStats);
        verify(mockOutput).writeOptionalEnumSet(infoStats);

        // 2 boolean writes, 1 for flatten, 1 for include metadata
        verify(mockOutput, times(2)).writeBoolean(true);
    }

    public void test_toXContent() throws IOException {
        List<String> nodeIds = Arrays.asList(NODE_ID_1);
        EnumSet<EventStatName> eventStats = EnumSet.of(EVENT_STAT);
        EnumSet<InfoStatName> infoStats = EnumSet.of(STATE_STAT);

        NeuralStatsInput input = NeuralStatsInput.builder()
            .nodeIds(nodeIds)
            .eventStatNames(eventStats)
            .infoStatNames(infoStats)
            .includeMetadata(true)
            .flatten(true)
            .build();

        XContentBuilder builder = XContentFactory.jsonBuilder();
        input.toXContent(builder, ToXContent.EMPTY_PARAMS);
        Map<String, Object> responseMap = xContentBuilderToMap(builder);

        assertEquals(Collections.singletonList(NODE_ID_1), responseMap.get("node_ids"));
        assertEquals(Collections.singletonList(EVENT_STAT.getNameString()), responseMap.get("event_stats"));
        assertEquals(Collections.singletonList(STATE_STAT.getNameString()), responseMap.get("state_stats"));
        assertEquals(true, responseMap.get(RestNeuralStatsAction.INCLUDE_METADATA_PARAM));
        assertEquals(true, responseMap.get(RestNeuralStatsAction.FLATTEN_PARAM));
    }

    public void test_writeToHandlesEmptyCollections() throws IOException {
        NeuralStatsInput input = new NeuralStatsInput();
        StreamOutput mockOutput = mock(StreamOutput.class);

        input.writeTo(mockOutput);

        verify(mockOutput).writeOptionalStringCollection(any(List.class));
        verify(mockOutput, times(2)).writeOptionalEnumSet(any(EnumSet.class));

        // 4 boolean writes, 2 for each enum set, 1 for flatten, 1 for include metadata
        verify(mockOutput, times(2)).writeBoolean(false);
    }
}
