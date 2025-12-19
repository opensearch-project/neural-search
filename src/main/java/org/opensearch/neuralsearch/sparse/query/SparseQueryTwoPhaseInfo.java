/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.query;

import com.google.common.annotations.VisibleForTesting;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.neuralsearch.query.NeuralSparseQueryTwoPhaseInfo;
import org.opensearch.neuralsearch.util.prune.PruneType;
import org.opensearch.neuralsearch.util.prune.PruneUtils;

import java.io.IOException;
import java.util.Locale;

/**
 * Configuration for two-phase sparse query execution.
 * Controls pruning strategy and window size for query optimization.
 */
@EqualsAndHashCode
@ToString
@Builder
@Getter
public class SparseQueryTwoPhaseInfo implements Writeable, ToXContent {
    @VisibleForTesting
    public static final ParseField TWO_PHASE_PARAMETERS_FIELD = new ParseField("two_phase_parameter");
    @VisibleForTesting
    public static final ParseField PRUNE_TYPE_FIELD = new ParseField("prune_type");
    @VisibleForTesting
    public static final ParseField PRUNE_RATIO_FIELD = new ParseField("prune_ratio");
    @VisibleForTesting
    public static final ParseField EXPANSION_RATE_FIELD = new ParseField("expansion_rate");
    @VisibleForTesting
    public static final ParseField MAX_WINDOW_SIZE_FIELD = new ParseField("max_window_size");
    public static final NeuralSparseQueryTwoPhaseInfo.TwoPhaseStatus DEFAULT_STATUS =
        NeuralSparseQueryTwoPhaseInfo.TwoPhaseStatus.NOT_ENABLED;
    public static final PruneType DEFAULT_PRUNE_TYPE = PruneType.MAX_RATIO;
    public static final float DEFAULT_PRUNE_RATIO = 0.4F;
    public static final float DEFAULT_EXPANSION_RATIO = 5.0F;
    public static final int DEFAULT_MAX_WINDOW_SIZE = 10000;

    private NeuralSparseQueryTwoPhaseInfo.TwoPhaseStatus status;
    private float twoPhasePruneRatio;
    private PruneType twoPhasePruneType;
    private float expansionRatio;
    private int maxWindowSize;

    /**
     * Creates instance with default configuration.
     */
    public SparseQueryTwoPhaseInfo() {
        this.status = DEFAULT_STATUS;
        this.twoPhasePruneRatio = DEFAULT_PRUNE_RATIO;
        this.twoPhasePruneType = DEFAULT_PRUNE_TYPE;
        this.expansionRatio = DEFAULT_EXPANSION_RATIO;
        this.maxWindowSize = DEFAULT_MAX_WINDOW_SIZE;
    }

    /**
     * Creates instance with specified parameters.
     *
     * @param status two-phase execution status
     * @param twoPhasePruneRatio ratio for pruning low-weight tokens
     * @param twoPhasePruneType pruning strategy type
     * @param expansionRatio candidate expansion multiplier
     * @param maxWindowSize maximum candidates in first phase
     */
    public SparseQueryTwoPhaseInfo(
        NeuralSparseQueryTwoPhaseInfo.TwoPhaseStatus status,
        float twoPhasePruneRatio,
        PruneType twoPhasePruneType,
        float expansionRatio,
        int maxWindowSize
    ) {
        this.status = status;
        this.twoPhasePruneRatio = twoPhasePruneRatio;
        this.twoPhasePruneType = twoPhasePruneType;
        this.expansionRatio = expansionRatio;
        this.maxWindowSize = maxWindowSize;
    }

    /**
     * Deserializes instance from stream.
     *
     * @param in input stream
     * @throws IOException if deserialization fails
     */
    public SparseQueryTwoPhaseInfo(StreamInput in) throws IOException {
        this.status = NeuralSparseQueryTwoPhaseInfo.TwoPhaseStatus.fromInt(in.readInt());
        this.twoPhasePruneRatio = in.readFloat();
        this.twoPhasePruneType = PruneType.fromString(in.readString());
        this.expansionRatio = in.readFloat();
        this.maxWindowSize = in.readInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(status.getValue());
        out.writeFloat(twoPhasePruneRatio);
        out.writeString(twoPhasePruneType.getValue());
        out.writeFloat(expansionRatio);
        out.writeInt(maxWindowSize);
    }

    /**
     * Parses two-phase info from XContent.
     *
     * @param parser XContent parser positioned at start object
     * @return parsed instance
     * @throws IOException if parsing fails
     */
    public static SparseQueryTwoPhaseInfo fromXContent(XContentParser parser) throws IOException {
        String methodFieldName = "";
        XContentParser.Token token = parser.currentToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new ParsingException(
                parser.getTokenLocation(),
                String.format(Locale.ROOT, "%s must be an object", TWO_PHASE_PARAMETERS_FIELD.getPreferredName())
            );
        }
        SparseQueryTwoPhaseInfo.SparseQueryTwoPhaseInfoBuilder builder = SparseQueryTwoPhaseInfo.builder();
        // set default value
        builder.status = DEFAULT_STATUS;
        builder.twoPhasePruneType = DEFAULT_PRUNE_TYPE;
        builder.twoPhasePruneRatio = DEFAULT_PRUNE_RATIO;
        builder.expansionRatio = DEFAULT_EXPANSION_RATIO;
        builder.maxWindowSize = DEFAULT_MAX_WINDOW_SIZE;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                methodFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (PRUNE_TYPE_FIELD.match(methodFieldName, parser.getDeprecationHandler())) {
                    builder.twoPhasePruneType = PruneType.fromString(parser.text());
                } else if (PRUNE_RATIO_FIELD.match(methodFieldName, parser.getDeprecationHandler())) {
                    builder.twoPhasePruneRatio = parser.floatValue();
                } else if (EXPANSION_RATE_FIELD.match(methodFieldName, parser.getDeprecationHandler())) {
                    builder.expansionRatio = parser.floatValue();
                    if (builder.expansionRatio < 1.0f) {
                        throw new ParsingException(
                            parser.getTokenLocation(),
                            String.format(
                                Locale.ROOT,
                                "[%s] %s must be >= 1.0",
                                TWO_PHASE_PARAMETERS_FIELD.getPreferredName(),
                                EXPANSION_RATE_FIELD.getPreferredName()
                            )
                        );
                    }
                } else if (MAX_WINDOW_SIZE_FIELD.match(methodFieldName, parser.getDeprecationHandler())) {
                    builder.maxWindowSize = parser.intValue();
                    if (builder.maxWindowSize < 50) {
                        throw new ParsingException(
                            parser.getTokenLocation(),
                            String.format(
                                Locale.ROOT,
                                "[%s] %s must be >= 50",
                                TWO_PHASE_PARAMETERS_FIELD.getPreferredName(),
                                MAX_WINDOW_SIZE_FIELD.getPreferredName()
                            )
                        );
                    }
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        String.format(
                            Locale.ROOT,
                            "[%s] unknown field [%s]",
                            TWO_PHASE_PARAMETERS_FIELD.getPreferredName(),
                            methodFieldName
                        )
                    );
                }
            } else {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    String.format(
                        Locale.ROOT,
                        "[%s] unknown token [%s] after [%s]",
                        TWO_PHASE_PARAMETERS_FIELD.getPreferredName(),
                        token,
                        methodFieldName
                    )
                );
            }
        }
        if (!PruneUtils.isValidPruneRatio(builder.twoPhasePruneType, builder.twoPhasePruneRatio)) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Illegal prune_ratio %f for prune_type: %s. %s",
                    builder.twoPhasePruneRatio,
                    builder.twoPhasePruneType.getValue(),
                    PruneUtils.getValidPruneRatioDescription(builder.twoPhasePruneType)
                )
            );
        }
        return builder.build();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(PRUNE_TYPE_FIELD.getPreferredName(), twoPhasePruneType.getValue());
        builder.field(PRUNE_RATIO_FIELD.getPreferredName(), twoPhasePruneRatio);
        builder.field(EXPANSION_RATE_FIELD.getPreferredName(), expansionRatio);
        builder.field(MAX_WINDOW_SIZE_FIELD.getPreferredName(), maxWindowSize);
        builder.endObject();
        return builder;
    }
}
