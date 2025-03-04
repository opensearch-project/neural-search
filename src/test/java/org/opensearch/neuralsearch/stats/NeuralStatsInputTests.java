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
import org.opensearch.neuralsearch.stats.events.EventStatName;
import org.opensearch.neuralsearch.stats.state.StateStatName;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class NeuralStatsInputTests extends OpenSearchTestCase {
    private static final String NODE_ID_1 = "node1";
    private static final String NODE_ID_2 = "node2";
    private static final EventStatName EVENT_STAT = EventStatName.TEXT_EMBEDDING_PROCESSOR_EXECUTIONS;
    private static final StateStatName STATE_STAT = StateStatName.TEXT_EMBEDDING_PROCESSORS;

    public void test_defaultConstructorEmpty() {
        NeuralStatsInput input = new NeuralStatsInput();

        assertTrue(input.getNodeIds().isEmpty());
        assertTrue(input.getEventStatNames().isEmpty());
        assertTrue(input.getStateStatNames().isEmpty());
        assertFalse(input.isIncludeMetadata());
        assertFalse(input.isFlatten());
    }

    public void test_builderWithAllFields() {
        Set<String> nodeIds = new HashSet<>(Arrays.asList(NODE_ID_1, NODE_ID_2));
        EnumSet<EventStatName> eventStats = EnumSet.of(EVENT_STAT);
        EnumSet<StateStatName> stateStats = EnumSet.of(STATE_STAT);

        NeuralStatsInput input = NeuralStatsInput.builder()
            .nodeIds(nodeIds)
            .eventStatNames(eventStats)
            .stateStatNames(stateStats)
            .includeMetadata(true)
            .flatten(true)
            .build();

        assertEquals(nodeIds, input.getNodeIds());
        assertEquals(eventStats, input.getEventStatNames());
        assertEquals(stateStats, input.getStateStatNames());
        assertTrue(input.isIncludeMetadata());
        assertTrue(input.isFlatten());
    }

    public void test_streamInput() throws IOException {
        StreamInput mockInput = mock(StreamInput.class);

        // Have to return the readByte since readBoolean can't be mocked
        when(mockInput.readByte()).thenReturn((byte) 1)   // true for nodeIds
            .thenReturn((byte) 1)   // true for eventStats
            .thenReturn((byte) 1)   // true for stateStats
            .thenReturn((byte) 1)   // true for includeMetadata
            .thenReturn((byte) 1);  // true for flatten

        when(mockInput.readStringList()).thenReturn(Arrays.asList(NODE_ID_1, NODE_ID_2));
        when(mockInput.readEnumSet(EventStatName.class)).thenReturn(EnumSet.of(EVENT_STAT));
        when(mockInput.readEnumSet(StateStatName.class)).thenReturn(EnumSet.of(STATE_STAT));

        NeuralStatsInput input = new NeuralStatsInput(mockInput);

        assertEquals(new HashSet<>(Arrays.asList(NODE_ID_1, NODE_ID_2)), input.getNodeIds());
        assertEquals(EnumSet.of(EVENT_STAT), input.getEventStatNames());
        assertEquals(EnumSet.of(STATE_STAT), input.getStateStatNames());
        assertTrue(input.isIncludeMetadata());
        assertTrue(input.isFlatten());

        verify(mockInput, times(5)).readByte();
        verify(mockInput, times(1)).readStringList();
        verify(mockInput, times(2)).readEnumSet(any());
    }

    public void test_writeToOutputs() throws IOException {
        Set<String> nodeIds = new HashSet<>(Arrays.asList(NODE_ID_1, NODE_ID_2));
        EnumSet<EventStatName> eventStats = EnumSet.of(EVENT_STAT);
        EnumSet<StateStatName> stateStats = EnumSet.of(STATE_STAT);

        NeuralStatsInput input = NeuralStatsInput.builder()
            .nodeIds(nodeIds)
            .eventStatNames(eventStats)
            .stateStatNames(stateStats)
            .includeMetadata(true)
            .flatten(true)
            .build();

        StreamOutput mockOutput = mock(StreamOutput.class);
        input.writeTo(mockOutput);

        verify(mockOutput).writeOptionalStringCollection(nodeIds);

        // 4 boolean writes, 2 for each enum set, 1 for flatten, 1 for include metadata
        verify(mockOutput, times(4)).writeBoolean(true);
        verify(mockOutput).writeEnumSet(eventStats);
        verify(mockOutput).writeEnumSet(stateStats);
    }

    public void test_toXContent() throws IOException {
        Set<String> nodeIds = new HashSet<>(Arrays.asList(NODE_ID_1));
        EnumSet<EventStatName> eventStats = EnumSet.of(EVENT_STAT);
        EnumSet<StateStatName> stateStats = EnumSet.of(STATE_STAT);

        NeuralStatsInput input = NeuralStatsInput.builder()
            .nodeIds(nodeIds)
            .eventStatNames(eventStats)
            .stateStatNames(stateStats)
            .includeMetadata(true)
            .flatten(true)
            .build();

        XContentBuilder builder = XContentFactory.jsonBuilder();
        input.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String jsonString = builder.toString();

        assertTrue(jsonString.contains(String.format("\"node_ids\":[\"%s\"]", NODE_ID_1)));
        assertTrue(jsonString.contains(String.format("\"event_stats\":[\"%s\"]", EVENT_STAT.getName())));
        assertTrue(jsonString.contains(String.format("\"state_stats\":[\"%s\"]", STATE_STAT.getName())));
        assertTrue(jsonString.contains("\"include_metadata\":true"));
        assertTrue(jsonString.contains("\"flat_keys\":true"));
    }

    public void test_writeToHandlesEmptyCollections() throws IOException {
        NeuralStatsInput input = new NeuralStatsInput();
        StreamOutput mockOutput = mock(StreamOutput.class);

        input.writeTo(mockOutput);

        verify(mockOutput).writeOptionalStringCollection(new HashSet<>());

        // 4 boolean writes, 2 for each enum set, 1 for flatten, 1 for include metadata
        verify(mockOutput, times(4)).writeBoolean(false);
    }
}
