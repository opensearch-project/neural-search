/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.ext;

import java.io.IOException;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

import org.opensearch.common.ParsingException;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.SearchExtBuilder;

/**
 * An extension class which will be used to read the Question Extension Object from Search request.
 * We will remove this extension when we have a way to create a Natural Language Question from OpenSearch Query DSL.
 */
@Log4j2
@EqualsAndHashCode(callSuper = false)
public class QuestionExtBuilder extends SearchExtBuilder {

    public static String NAME = "question_extension";

    private static final ParseField QUESTION_FIELD = new ParseField("question");

    @Getter
    @Setter
    private String question;

    /**
     * Returns the name of the writeable object
     */
    @Override
    public String getWriteableName() {
        return NAME;
    }

    /**
     * Write this into the {@linkplain StreamOutput}.
     *
     * @param out {@link StreamOutput}
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(question);
    }

    public QuestionExtBuilder() {

    }

    public QuestionExtBuilder(StreamInput in) throws IOException {
        String question = in.readString();
        setQuestion(question);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(QUESTION_FIELD.getPreferredName(), question);
        return builder;
    }

    public static QuestionExtBuilder parse(XContentParser parser) throws IOException {
        final QuestionExtBuilder questionExtBuilder = new QuestionExtBuilder();
        XContentParser.Token token = parser.currentToken();
        String currentFieldName = null;
        if (token != XContentParser.Token.START_OBJECT && (token = parser.nextToken()) != XContentParser.Token.START_OBJECT) {
            throw new ParsingException(
                parser.getTokenLocation(),
                "Expected [" + XContentParser.Token.START_OBJECT + "] but found [" + token + "]",
                parser.getTokenLocation()
            );
        }
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (QUESTION_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    questionExtBuilder.setQuestion(parser.text());
                }
            } else {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "Unknown key for a " + token + " in [" + currentFieldName + "].",
                    parser.getTokenLocation()
                );
            }
        }

        return questionExtBuilder;
    }
}
