/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import com.google.common.annotations.VisibleForTesting;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.opensearch.Version;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.neuralsearch.settings.NeuralSearchSettings;
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

/**
 * Represents the parameters for neural_sparse two-phase process.
 * This class encapsulates settings related to window size expansion, pruning ratio, and whether the two-phase search is enabled.
 * It includes mechanisms to update settings from the cluster dynamically.
 */
@Getter
@Setter
@Accessors(chain = true, fluent = true)
@NoArgsConstructor
@AllArgsConstructor
public class NeuralSparseTwoPhaseParameters implements Writeable {
    @VisibleForTesting
    static final ParseField NAME = new ParseField("two_phase_settings");
    @VisibleForTesting
    static final ParseField WINDOW_SIZE_EXPANSION = new ParseField("window_size_expansion");
    @VisibleForTesting
    static final ParseField PRUNING_RATIO = new ParseField("pruning_ratio");
    @VisibleForTesting
    static final ParseField ENABLED = new ParseField("enabled");
    private static final Version MINIMAL_SUPPORTED_VERSION_TWO_PHASE_SEARCH = Version.CURRENT;
    private Float window_size_expansion;
    private Float pruning_ratio;
    private Boolean enabled;

    /**
     * Constructor from index's setting
     *
     * @param settings setting of index
     */
    public NeuralSparseTwoPhaseParameters(final Settings settings) {
        this.enabled(NeuralSearchSettings.NEURAL_SPARSE_TWO_PHASE_DEFAULT_ENABLED.get(settings));
        this.pruning_ratio(NeuralSearchSettings.NEURAL_SPARSE_TWO_PHASE_DEFAULT_PRUNING_RATIO.get(settings));
        this.window_size_expansion(NeuralSearchSettings.NEURAL_SPARSE_TWO_PHASE_DEFAULT_WINDOW_SIZE_EXPANSION.get(settings));
    }

    /**
     * Constructor from stream input
     *
     * @param in StreamInput to initialize object from
     * @throws IOException thrown if unable to read from input stream
     */
    public NeuralSparseTwoPhaseParameters(final StreamInput in) throws IOException {
        window_size_expansion = in.readOptionalFloat();
        pruning_ratio = in.readOptionalFloat();
        enabled = in.readOptionalBoolean();
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeOptionalFloat(window_size_expansion);
        out.writeOptionalFloat(pruning_ratio);
        out.writeOptionalBoolean(enabled);
    }

    /**
     * Builds the content of this object into an XContentBuilder, typically for JSON serialization.
     *
     * @param builder The builder to fill.
     * @throws IOException if building the content fails.
     */
    public void doXContent(final XContentBuilder builder) throws IOException {
        builder.startObject(NAME.getPreferredName());
        builder.field(WINDOW_SIZE_EXPANSION.getPreferredName(), window_size_expansion);
        builder.field(PRUNING_RATIO.getPreferredName(), pruning_ratio);
        builder.field(ENABLED.getPreferredName(), enabled);
        builder.endObject();
    }

    /**
     * Parses a NeuralSparseTwoPhaseParameters object from XContent (typically JSON).
     *
     * @param parser the XContentParser to extract data from.
     * @return a new instance of NeuralSparseTwoPhaseParameters initialized from the parser.
     * @throws IOException if parsing fails.
     */
    public static NeuralSparseTwoPhaseParameters parseFromXContent(final XContentParser parser) throws IOException {
        XContentParser.Token token;
        String currentFieldName = "";
        NeuralSparseTwoPhaseParameters neuralSparseTwoPhaseParameters = new NeuralSparseTwoPhaseParameters();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (WINDOW_SIZE_EXPANSION.match(currentFieldName, parser.getDeprecationHandler())) {
                    Float windowSizeExpansion = parser.floatValue();
                    neuralSparseTwoPhaseParameters.window_size_expansion(windowSizeExpansion);
                } else if (PRUNING_RATIO.match(currentFieldName, parser.getDeprecationHandler())) {
                    Float pruningRatio = parser.floatValue();
                    neuralSparseTwoPhaseParameters.pruning_ratio(pruningRatio);
                } else if (ENABLED.match(currentFieldName, parser.getDeprecationHandler())) {
                    Boolean enabled = parser.booleanValue();
                    neuralSparseTwoPhaseParameters.enabled(enabled);
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        String.format(Locale.ROOT, "[%s] does not support [%s] field", NAME.getPreferredName(), currentFieldName)
                    );
                }
            } else {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    String.format(Locale.ROOT, "[%s] unknown token [%s] after [%s]", NAME.getPreferredName(), token, currentFieldName)
                );
            }
        }

        return neuralSparseTwoPhaseParameters;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        NeuralSparseTwoPhaseParameters other = (NeuralSparseTwoPhaseParameters) obj;
        return Objects.equals(enabled, other.enabled)
            && Objects.equals(window_size_expansion, other.window_size_expansion)
            && Objects.equals(pruning_ratio, other.pruning_ratio);
    }

    @Override
    public int hashCode() {
        HashCodeBuilder builder = new HashCodeBuilder().append(enabled).append(window_size_expansion).append(pruning_ratio);
        return builder.toHashCode();
    }

    /**
     * Checks if the two-phase search feature is enabled based on the given parameters.
     *
     * @param neuralSparseTwoPhaseParameters The parameters to check.
     * @return true if enabled, false otherwise.
     */
    public static boolean isEnabled(final NeuralSparseTwoPhaseParameters neuralSparseTwoPhaseParameters) {
        if (!isClusterOnSameVersionForTwoPhaseSearchSupport() || Objects.isNull(neuralSparseTwoPhaseParameters)) {
            return false;
        }
        return neuralSparseTwoPhaseParameters.enabled();
    }

    /**
     * A flag to determine if this feature are support.
     *
     * @return True if cluster are on support, false if it doesn't.
     */
    public static boolean isClusterOnSameVersionForTwoPhaseSearchSupport() {
        NeuralSearchClusterUtil neuralSearchClusterUtil = NeuralSearchClusterUtil.instance();
        return neuralSearchClusterUtil.getClusterMinVersion() == neuralSearchClusterUtil.getClusterMaxVersion();
    }

    /**
     * Check if neuralSparseTwoPhaseParameters's value are valid.
     * @param neuralSparseTwoPhaseParameters the parameter to check
     */
    public static void checkValidValueOfTwoPhaseParameter(NeuralSparseTwoPhaseParameters neuralSparseTwoPhaseParameters) {
        if (neuralSparseTwoPhaseParameters == null) return;

        if (neuralSparseTwoPhaseParameters.pruning_ratio() < 0 || neuralSparseTwoPhaseParameters.pruning_ratio() > 1) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "[%s] %s field value must be in range [0,1]",
                    NeuralSparseTwoPhaseParameters.NAME.getPreferredName(),
                    NeuralSparseTwoPhaseParameters.PRUNING_RATIO.getPreferredName()
                )
            );
        }

        if (neuralSparseTwoPhaseParameters.window_size_expansion() < 1) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "[%s] %s field value can not be smaller than 1",
                    NeuralSparseTwoPhaseParameters.NAME.getPreferredName(),
                    NeuralSparseTwoPhaseParameters.WINDOW_SIZE_EXPANSION.getPreferredName()
                )
            );
        }
    }
}
