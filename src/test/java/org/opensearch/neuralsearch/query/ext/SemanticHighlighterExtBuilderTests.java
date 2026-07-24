/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query.ext;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.SearchExtBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;

public class SemanticHighlighterExtBuilderTests extends OpenSearchTestCase {

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(
            List.of(
                new NamedXContentRegistry.Entry(
                    SearchExtBuilder.class,
                    new ParseField(SemanticHighlighterExtBuilder.NAME),
                    SemanticHighlighterExtBuilder::parse
                )
            )
        );
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return new NamedWriteableRegistry(
            List.of(
                new NamedWriteableRegistry.Entry(
                    SearchExtBuilder.class,
                    SemanticHighlighterExtBuilder.NAME,
                    SemanticHighlighterExtBuilder::new
                )
            )
        );
    }

    public void testWriteableNameMatchesConstant() {
        SemanticHighlighterExtBuilder builder = new SemanticHighlighterExtBuilder(true);
        assertEquals(SemanticHighlighterExtBuilder.NAME, builder.getWriteableName());
    }

    public void testRoundTripStreamingTrue() throws IOException {
        roundTrip(true);
    }

    public void testRoundTripStreamingFalse() throws IOException {
        roundTrip(false);
    }

    public void testRoundTripXContentTrue() throws IOException {
        roundTripXContent(true);
    }

    public void testRoundTripXContentFalse() throws IOException {
        roundTripXContent(false);
    }

    public void testParseBooleanTrue() throws IOException {
        SemanticHighlighterExtBuilder result = parseValue("true");
        assertTrue(result.isEnabled());
    }

    public void testParseBooleanFalse() throws IOException {
        SemanticHighlighterExtBuilder result = parseValue("false");
        assertFalse(result.isEnabled());
    }

    public void testParseStringTrue() throws IOException {
        SemanticHighlighterExtBuilder result = parseValue("\"true\"");
        assertTrue(result.isEnabled());
    }

    public void testParseRejectsNonBoolean() {
        IOException e = expectThrows(IOException.class, () -> parseValue("123"));
        assertTrue(e.getMessage().contains("must be a boolean"));
    }

    public void testEqualsHashCode() {
        SemanticHighlighterExtBuilder a = new SemanticHighlighterExtBuilder(true);
        SemanticHighlighterExtBuilder b = new SemanticHighlighterExtBuilder(true);
        SemanticHighlighterExtBuilder c = new SemanticHighlighterExtBuilder(false);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
    }

    private void roundTrip(boolean value) throws IOException {
        SemanticHighlighterExtBuilder original = new SemanticHighlighterExtBuilder(value);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                SemanticHighlighterExtBuilder deserialized = new SemanticHighlighterExtBuilder(in);
                assertEquals(original, deserialized);
            }
        }
    }

    private void roundTripXContent(boolean value) throws IOException {
        SemanticHighlighterExtBuilder original = new SemanticHighlighterExtBuilder(value);
        MediaType xContentType = randomFrom(XContentType.values());
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().ext(List.of(original));
        assertEquals("{\"ext\":{\"semantic_highlighting_batch\":" + value + "}}", sourceBuilder.toString());
        boolean humanReadable = randomBoolean();
        BytesReference shuffledXContent = this.toShuffledXContent(
            sourceBuilder,
            xContentType,
            ToXContentObject.EMPTY_PARAMS,
            humanReadable
        );

        try (XContentParser parser = this.createParser(xContentType.xContent(), shuffledXContent)) {
            SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(parser);
            assertEquals(1, searchSourceBuilder.ext().size());
            assertEquals(original, searchSourceBuilder.ext().getFirst());
        }
    }

    private SemanticHighlighterExtBuilder parseValue(String json) throws IOException {
        XContentParser parser = createParser(org.opensearch.common.xcontent.XContentType.JSON.xContent(), json);
        // advance past START_TOKEN to the value
        parser.nextToken();
        return SemanticHighlighterExtBuilder.parse(parser);
    }
}
