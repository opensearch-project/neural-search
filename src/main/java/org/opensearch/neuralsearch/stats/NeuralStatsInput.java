/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats;

import lombok.Builder;
import lombok.Getter;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

// TODO : Not yet implemented
@Getter
public class NeuralStatsInput implements ToXContentObject, Writeable {
    public static final String NODE_IDS = "node_ids";

    /**
     * Which node's stats will be retrieved.
     */
    private Set<String> nodeIds;

    @Builder
    public NeuralStatsInput(Set<String> nodeIds) {
        this.nodeIds = nodeIds;
    }

    public NeuralStatsInput() {
        this.nodeIds = new HashSet<>();
    }

    public NeuralStatsInput(StreamInput input) throws IOException {
        nodeIds = input.readBoolean() ? new HashSet<>(input.readStringList()) : new HashSet<>();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalStringCollection(nodeIds);
    }

    private void writeEnumSet(StreamOutput out, EnumSet<?> set) throws IOException {
        if (set != null && set.size() > 0) {
            out.writeBoolean(true);
            out.writeEnumSet(set);
        } else {
            out.writeBoolean(false);
        }
    }

    public static NeuralStatsInput parse(XContentParser parser) throws IOException {
        Set<String> nodeIds = new HashSet<>();

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case NODE_IDS:
                    parseArrayField(parser, nodeIds);
                    break;
                default:
                    parser.skipChildren();
                    break;
            }
        }
        return NeuralStatsInput.builder().nodeIds(nodeIds).build();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        if (nodeIds != null) {
            builder.field(NODE_IDS, nodeIds);
        }
        builder.endObject();
        return builder;
    }

    public boolean retrieveStatsOnAllNodes() {
        return nodeIds == null || nodeIds.size() == 0;
    }

    public static void parseArrayField(XContentParser parser, Set<String> set) throws IOException {
        parseField(parser, set, null, String.class);
    }

    public static <T> void parseField(XContentParser parser, Set<T> set, Function<String, T> function, Class<T> clazz) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            String value = parser.text();
            if (function != null) {
                set.add(function.apply(value));
            } else {
                if (clazz.isInstance(value)) {
                    set.add(clazz.cast(value));
                }
            }
        }
    }
}
