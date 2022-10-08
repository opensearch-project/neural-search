/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.transport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.log4j.Log4j2;

import org.opensearch.action.ActionResponse;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

@Data
@EqualsAndHashCode(callSuper = false)
@AllArgsConstructor
@Log4j2
public class MLPredictActionResponse extends ActionResponse implements ToXContentObject {

    private static final String INFERENCE_VECTORS = "inference_vectors";

    private List<List<Float>> inferenceVectorsList;

    public MLPredictActionResponse(StreamInput streamInput) throws IOException {
        super(streamInput);
        final int inferenceVectorsListSize = streamInput.readVInt();
        inferenceVectorsList = new ArrayList<>(inferenceVectorsListSize);
        for (int i = 0; i < inferenceVectorsListSize; i++) {
            final int vectorSize = streamInput.readVInt();
            final List<Float> vector = new ArrayList<>();
            for (int j = 0; j < vectorSize; j++) {
                vector.add(streamInput.readFloat());
            }
            inferenceVectorsList.add(vector);
        }
    }

    @Override
    public void writeTo(StreamOutput streamOutput) throws IOException {
        streamOutput.writeVInt(inferenceVectorsList.size());
        for (final List<Float> vector : inferenceVectorsList) {
            streamOutput.writeCollection(vector, StreamOutput::writeFloat);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder xContentBuilder, Params params) throws IOException {
        xContentBuilder.startObject().startArray(INFERENCE_VECTORS);
        for (final List<Float> floats : inferenceVectorsList) {
            xContentBuilder.startArray();
            for (final Float value : floats) {
                xContentBuilder.value(value);
            }
            xContentBuilder.endArray();
        }
        return xContentBuilder.endArray().endObject();
    }
}
