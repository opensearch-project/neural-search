/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.neuralsearch.query.NeuralSparseQueryTwoPhaseInfo;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;
import org.opensearch.neuralsearch.util.prune.PruneType;

import java.io.IOException;
import java.nio.charset.Charset;

public class SparseQueryTwoPhaseInfoTests extends AbstractSparseTestBase {

    public void testDefaultConstructor() {
        SparseQueryTwoPhaseInfo info = new SparseQueryTwoPhaseInfo();
        assertEquals(NeuralSparseQueryTwoPhaseInfo.TwoPhaseStatus.NOT_ENABLED, info.getStatus());
        assertEquals(0.4f, info.getTwoPhasePruneRatio(), DELTA_FOR_ASSERTION);
        assertEquals(PruneType.MAX_RATIO, info.getTwoPhasePruneType());
        assertEquals(5.0f, info.getExpansionRatio(), DELTA_FOR_ASSERTION);
        assertEquals(10000, info.getMaxWindowSize());
    }

    public void testParameterizedConstructor() {
        SparseQueryTwoPhaseInfo info = new SparseQueryTwoPhaseInfo(
            NeuralSparseQueryTwoPhaseInfo.TwoPhaseStatus.PHASE_ONE,
            0.5f,
            PruneType.ABS_VALUE,
            3.0f,
            5000
        );
        assertEquals(NeuralSparseQueryTwoPhaseInfo.TwoPhaseStatus.PHASE_ONE, info.getStatus());
        assertEquals(0.5f, info.getTwoPhasePruneRatio(), DELTA_FOR_ASSERTION);
        assertEquals(PruneType.ABS_VALUE, info.getTwoPhasePruneType());
        assertEquals(3.0f, info.getExpansionRatio(), DELTA_FOR_ASSERTION);
        assertEquals(5000, info.getMaxWindowSize());
    }

    public void testStreamSerialization() throws IOException {
        SparseQueryTwoPhaseInfo original = SparseQueryTwoPhaseInfo.builder()
            .status(NeuralSparseQueryTwoPhaseInfo.TwoPhaseStatus.PHASE_ONE)
            .twoPhasePruneRatio(0.6f)
            .twoPhasePruneType(PruneType.TOP_K)
            .expansionRatio(4.0f)
            .maxWindowSize(8000)
            .build();

        BytesStreamOutput output = new BytesStreamOutput();
        original.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        SparseQueryTwoPhaseInfo deserialized = new SparseQueryTwoPhaseInfo(input);

        assertEquals(original.getStatus(), deserialized.getStatus());
        assertEquals(original.getTwoPhasePruneRatio(), deserialized.getTwoPhasePruneRatio(), DELTA_FOR_ASSERTION);
        assertEquals(original.getTwoPhasePruneType(), deserialized.getTwoPhasePruneType());
        assertEquals(original.getExpansionRatio(), deserialized.getExpansionRatio(), DELTA_FOR_ASSERTION);
        assertEquals(original.getMaxWindowSize(), deserialized.getMaxWindowSize());
    }

    public void testFromXContent_withValidJson() throws IOException {
        String json = "{\"prune_type\": \"max_ratio\", \"prune_ratio\": 0.5, \"expansion_rate\": 3.0, \"max_window_size\": 5000}";
        XContentParser parser = createParser(json);
        parser.nextToken();

        SparseQueryTwoPhaseInfo info = SparseQueryTwoPhaseInfo.fromXContent(parser);

        assertEquals(PruneType.MAX_RATIO, info.getTwoPhasePruneType());
        assertEquals(0.5f, info.getTwoPhasePruneRatio(), DELTA_FOR_ASSERTION);
        assertEquals(3.0f, info.getExpansionRatio(), DELTA_FOR_ASSERTION);
        assertEquals(5000, info.getMaxWindowSize());
    }

    public void testFromXContent_withDefaultValues() throws IOException {
        String json = "{}";
        XContentParser parser = createParser(json);
        parser.nextToken();

        SparseQueryTwoPhaseInfo info = SparseQueryTwoPhaseInfo.fromXContent(parser);

        assertEquals(PruneType.MAX_RATIO, info.getTwoPhasePruneType());
        assertEquals(0.4f, info.getTwoPhasePruneRatio(), DELTA_FOR_ASSERTION);
        assertEquals(5.0f, info.getExpansionRatio(), DELTA_FOR_ASSERTION);
        assertEquals(10000, info.getMaxWindowSize());
    }

    public void testFromXContent_withInvalidStartToken() throws IOException {
        String json = "\"invalid\"";
        XContentParser parser = createParser(json);
        parser.nextToken();

        ParsingException exception = expectThrows(ParsingException.class, () -> SparseQueryTwoPhaseInfo.fromXContent(parser));
        assertTrue(exception.getMessage().contains("must be an object"));
    }

    public void testFromXContent_withUnknownField() throws IOException {
        String json = "{\"unknown_field\": \"value\"}";
        XContentParser parser = createParser(json);
        parser.nextToken();

        ParsingException exception = expectThrows(ParsingException.class, () -> SparseQueryTwoPhaseInfo.fromXContent(parser));
        assertTrue(exception.getMessage().contains("unknown field"));
    }

    public void testFromXContent_withInvalidExpansionRate() throws IOException {
        String json = "{\"expansion_rate\": 0.5}";
        XContentParser parser = createParser(json);
        parser.nextToken();

        ParsingException exception = expectThrows(ParsingException.class, () -> SparseQueryTwoPhaseInfo.fromXContent(parser));
        assertTrue(exception.getMessage().contains("must be >= 1.0"));
    }

    public void testFromXContent_withInvalidMaxWindowSize() throws IOException {
        String json = "{\"max_window_size\": 30}";
        XContentParser parser = createParser(json);
        parser.nextToken();

        ParsingException exception = expectThrows(ParsingException.class, () -> SparseQueryTwoPhaseInfo.fromXContent(parser));
        assertTrue(exception.getMessage().contains("must be >= 50"));
    }

    public void testFromXContent_withInvalidPruneRatio() throws IOException {
        String json = "{\"prune_type\": \"max_ratio\", \"prune_ratio\": 2.0}";
        XContentParser parser = createParser(json);
        parser.nextToken();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> SparseQueryTwoPhaseInfo.fromXContent(parser)
        );
        assertTrue(exception.getMessage().contains("Illegal prune_ratio"));
    }

    public void testFromXContent_withUnknownToken() throws IOException {
        String json = "{\"prune_ratio\": [1, 2, 3]}";
        XContentParser parser = createParser(json);
        parser.nextToken();

        ParsingException exception = expectThrows(ParsingException.class, () -> SparseQueryTwoPhaseInfo.fromXContent(parser));
        assertTrue(exception.getMessage().contains("unknown token"));
    }

    public void testEqualsAndHashCode() {
        SparseQueryTwoPhaseInfo info1 = SparseQueryTwoPhaseInfo.builder()
            .status(NeuralSparseQueryTwoPhaseInfo.TwoPhaseStatus.PHASE_ONE)
            .twoPhasePruneRatio(0.5f)
            .twoPhasePruneType(PruneType.MAX_RATIO)
            .expansionRatio(3.0f)
            .maxWindowSize(5000)
            .build();

        SparseQueryTwoPhaseInfo info2 = SparseQueryTwoPhaseInfo.builder()
            .status(NeuralSparseQueryTwoPhaseInfo.TwoPhaseStatus.PHASE_ONE)
            .twoPhasePruneRatio(0.5f)
            .twoPhasePruneType(PruneType.MAX_RATIO)
            .expansionRatio(3.0f)
            .maxWindowSize(5000)
            .build();

        SparseQueryTwoPhaseInfo info3 = SparseQueryTwoPhaseInfo.builder()
            .status(NeuralSparseQueryTwoPhaseInfo.TwoPhaseStatus.NOT_ENABLED)
            .twoPhasePruneRatio(0.6f)
            .twoPhasePruneType(PruneType.ABS_VALUE)
            .expansionRatio(4.0f)
            .maxWindowSize(6000)
            .build();

        assertEquals(info1, info2);
        assertEquals(info1.hashCode(), info2.hashCode());
        assertNotEquals(info1, info3);
    }

    private XContentParser createParser(String json) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .rawValue(new java.io.ByteArrayInputStream(json.getBytes(Charset.defaultCharset())), XContentType.JSON);
        return createParser(builder);
    }
}
