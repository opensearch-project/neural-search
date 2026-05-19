/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query.ext;

import java.io.IOException;
import java.util.List;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.SearchExtBuilder;
import org.opensearch.test.OpenSearchTestCase;

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

    private SemanticHighlighterExtBuilder parseValue(String json) throws IOException {
        XContentParser parser = createParser(org.opensearch.common.xcontent.XContentType.JSON.xContent(), json);
        // advance past START_TOKEN to the value
        parser.nextToken();
        return SemanticHighlighterExtBuilder.parse(parser);
    }
}
