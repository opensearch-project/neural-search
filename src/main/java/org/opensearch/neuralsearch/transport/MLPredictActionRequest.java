/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.transport;

import java.io.IOException;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ValidateActions;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
@ToString
public class MLPredictActionRequest extends ActionRequest {
    @Getter
    private final String modelId;
    @Getter
    private final List<String> inputSentencesList;

    public MLPredictActionRequest(final StreamInput in) throws IOException {
        super(in);
        this.modelId = in.readString();
        this.inputSentencesList = in.readStringList();
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(modelId);
        out.writeStringCollection(inputSentencesList);
    }

    @Override
    public ActionRequestValidationException validate() {
        if (!Strings.hasText(modelId)) {
            return ValidateActions.addValidationError("Model id cannot be empty ", null);
        }
        if (inputSentencesList.size() == 0) {
            return ValidateActions.addValidationError("Input Sentences List cannot be empty ", null);
        }
        return null;
    }
}
