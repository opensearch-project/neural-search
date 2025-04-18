/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import lombok.Getter;
import lombok.Setter;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.neuralsearch.util.prune.PruneType;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This class encapsulates information related to the two-phase execution process
 * for a neural sparse query. It tracks the current processing phase, the ratio
 * used for pruning during the two-phase process, and the type of pruning applied.
 */
@Getter
@Setter
public class NeuralSparseQueryTwoPhaseInfo implements Writeable {
    private TwoPhaseStatus status = TwoPhaseStatus.NOT_ENABLED;
    private float twoPhasePruneRatio = 0F;
    private PruneType twoPhasePruneType = PruneType.NONE;

    NeuralSparseQueryTwoPhaseInfo() {}

    NeuralSparseQueryTwoPhaseInfo(TwoPhaseStatus status, float twoPhasePruneRatio, PruneType twoPhasePruneType) {
        this.status = status;
        this.twoPhasePruneRatio = twoPhasePruneRatio;
        this.twoPhasePruneType = twoPhasePruneType;
    }

    NeuralSparseQueryTwoPhaseInfo(StreamInput in) throws IOException {
        this.status = TwoPhaseStatus.fromInt(in.readInt());
        this.twoPhasePruneRatio = in.readFloat();
        this.twoPhasePruneType = PruneType.fromString(in.readString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(status.getValue());
        out.writeFloat(twoPhasePruneRatio);
        out.writeString(twoPhasePruneType.getValue());
    }

    public enum TwoPhaseStatus {
        NOT_ENABLED(0),
        PHASE_ONE(1),
        PHASE_TWO(2);

        private static final Map<Integer, TwoPhaseStatus> VALUE_MAP = Arrays.stream(values())
            .collect(Collectors.toUnmodifiableMap(status -> status.value, Function.identity()));
        private final int value;

        TwoPhaseStatus(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        /**
         * Converts an integer value to the corresponding TwoPhaseStatus.
         * @param value the integer value to convert
         * @return the corresponding TwoPhaseStatus enum constant
         * @throws IllegalArgumentException if the value does not correspond to any known status
         */
        public static TwoPhaseStatus fromInt(final int value) {
            TwoPhaseStatus status = VALUE_MAP.get(value);
            if (status == null) {
                throw new IllegalArgumentException(String.format(Locale.ROOT, "Invalid two phase status value: %d", value));
            }
            return status;
        }
    }
}
