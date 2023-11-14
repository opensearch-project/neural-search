/*
 * Copyright 2023 Aryn
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opensearch.neuralsearch.query.ext;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import lombok.AllArgsConstructor;
import lombok.Getter;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.SearchExtBuilder;

@AllArgsConstructor
public class RerankSearchExtBuilder extends SearchExtBuilder {

    public final static String PARAM_FIELD_NAME = "rerank";
    @Getter
    protected Map<String, Object> params;

    public RerankSearchExtBuilder(StreamInput in) throws IOException {
        params = in.readMap();
    }

    @Override
    public String getWriteableName() {
        return PARAM_FIELD_NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(params);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(PARAM_FIELD_NAME, this.params);
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.getClass(), this.params);
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof RerankSearchExtBuilder) && params.equals(((RerankSearchExtBuilder) obj).params);
    }

    /**
     * Pick out the first RerankSearchExtBuilder from a list of SearchExtBuilders
     * @param builders list of SearchExtBuilders
     * @return the RerankSearchExtBuilder
     */
    public static RerankSearchExtBuilder fromExtBuilderList(List<SearchExtBuilder> builders) {
        Optional<SearchExtBuilder> b = builders.stream().filter(bldr -> bldr instanceof RerankSearchExtBuilder).findFirst();
        if (b.isPresent()) {
            return (RerankSearchExtBuilder) b.get();
        } else {
            return null;
        }
    }

    /**
     * Parse XContent to rerankSearchExtBuilder
     * @param parser parser parsing this searchExt
     * @return RerankSearchExtBuilder represented by this searchExt
     * @throws IOException if problems parsing
     */
    public static RerankSearchExtBuilder parse(XContentParser parser) throws IOException {
        return new RerankSearchExtBuilder(parser.map());
    }

}
