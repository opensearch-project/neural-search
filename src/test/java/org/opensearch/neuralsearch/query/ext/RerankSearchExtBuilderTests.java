/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query.ext;

import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.search.SearchExtBuilder;
import org.opensearch.test.OpenSearchTestCase;

import lombok.extern.log4j.Log4j2;

@Log4j2
public class RerankSearchExtBuilderTests extends OpenSearchTestCase {

    Map<String, Object> params;

    @Before
    public void setup() {
        params = Map.of("query_text", "question about the meaning of life, the universe, and everything");
    }

    public void testStreaming() throws IOException {
        RerankSearchExtBuilder b1 = new RerankSearchExtBuilder(params);
        BytesStreamOutput outbytes = new BytesStreamOutput();
        StreamOutput osso = new OutputStreamStreamOutput(outbytes);
        b1.writeTo(osso);
        StreamInput in = new BytesStreamInput(BytesReference.toBytes(outbytes.bytes()));
        RerankSearchExtBuilder b2 = new RerankSearchExtBuilder(in);
        assert (b2.getParams().equals(params));
        assert (b1.equals(b2));
    }

    // public void testToXContent() throws IOException {
    // RerankSearchExtBuilder b1 = new RerankSearchExtBuilder(new HashMap<>(params));
    // XContentBuilder builder = XContentType.JSON.contentBuilder();
    // builder.startObject();
    // b1.toXContent(builder, ToXContentObject.EMPTY_PARAMS);
    // builder.endObject();
    // String extString = builder.toString();
    // log.info(extString);
    // XContentParser parser = this.createParser(XContentType.JSON.xContent(), extString);
    // RerankSearchExtBuilder b2 = RerankSearchExtBuilder.parse(parser);
    // assert (b2.getParams().equals(params));
    // assert (b1.equals(b2));
    // }

    public void testPullFromListOfExtBuilders() {
        RerankSearchExtBuilder builder = new RerankSearchExtBuilder(params);
        SearchExtBuilder otherBuilder = mock(SearchExtBuilder.class);
        assert (!builder.equals(otherBuilder));
        List<SearchExtBuilder> builders1 = List.of(otherBuilder, builder);
        List<SearchExtBuilder> builders2 = List.of(otherBuilder);
        List<SearchExtBuilder> builders3 = List.of();
        assert (RerankSearchExtBuilder.fromExtBuilderList(builders1).equals(builder));
        assert (RerankSearchExtBuilder.fromExtBuilderList(builders2) == null);
        assert (RerankSearchExtBuilder.fromExtBuilderList(builders3) == null);
    }

    public void testHash() {
        RerankSearchExtBuilder b1 = new RerankSearchExtBuilder(params);
        RerankSearchExtBuilder b2 = new RerankSearchExtBuilder(params);
        RerankSearchExtBuilder b3 = new RerankSearchExtBuilder(Map.of());
        assert (b1.hashCode() == b2.hashCode());
        assert (b1.hashCode() != b3.hashCode());
        assert (!b1.equals(b3));
    }

    public void testWriteableName() {
        RerankSearchExtBuilder b1 = new RerankSearchExtBuilder(params);
        assert (b1.getWriteableName().equals(RerankSearchExtBuilder.PARAM_FIELD_NAME));
    }
}
