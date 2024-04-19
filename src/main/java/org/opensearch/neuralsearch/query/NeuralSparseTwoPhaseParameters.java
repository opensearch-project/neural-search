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
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.neuralsearch.settings.NeuralSearchSettings;
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

@Getter
@Setter
@Accessors(chain = true, fluent = true)
@NoArgsConstructor
@AllArgsConstructor
public class NeuralSparseTwoPhaseParameters implements ToXContentObject, Writeable {
    public static volatile Float DEFAULT_WINDOW_SIZE_EXPANSION;
    public static volatile Float DEFAULT_PRUNING_RATIO;
    public static volatile Boolean DEFAULT_ENABLED;
    public static volatile Integer MAX_WINDOW_SIZE;
    @VisibleForTesting
    static final ParseField NAME = new ParseField("two_phase_settings");
    @VisibleForTesting
    static final ParseField WINDOW_SIZE_EXPANSION = new ParseField("window_size_expansion");
    @VisibleForTesting
    static final ParseField PRUNING_RATIO = new ParseField("pruning_ratio");
    @VisibleForTesting
    static final ParseField ENABLED = new ParseField("enabled");
    private static final Version MINIMAL_SUPPORTED_VERSION_TWO_PHASE_SEARCH = Version.V_2_14_0;

    private Float window_size_expansion;
    private Float pruning_ratio;
    private Boolean enabled;

    public static void initialize(ClusterService clusterService, Settings settings) {
        DEFAULT_ENABLED = NeuralSearchSettings.NEURAL_SPARSE_TWO_PHASE_DEFAULT_ENABLED.get(settings);
        DEFAULT_WINDOW_SIZE_EXPANSION = NeuralSearchSettings.NEURAL_SPARSE_TWO_PHASE_DEFAULT_WINDOW_SIZE_EXPANSION.get(settings);
        DEFAULT_PRUNING_RATIO = NeuralSearchSettings.NEURAL_SPARSE_TWO_PHASE_DEFAULT_PRUNING_RATIO.get(settings);
        MAX_WINDOW_SIZE = NeuralSearchSettings.NEURAL_SPARSE_TWO_PHASE_MAX_WINDOW_SIZE.get(settings);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(NeuralSearchSettings.NEURAL_SPARSE_TWO_PHASE_DEFAULT_ENABLED, it -> DEFAULT_ENABLED = it);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(
                NeuralSearchSettings.NEURAL_SPARSE_TWO_PHASE_DEFAULT_WINDOW_SIZE_EXPANSION,
                it -> DEFAULT_WINDOW_SIZE_EXPANSION = it
            );
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(
                NeuralSearchSettings.NEURAL_SPARSE_TWO_PHASE_DEFAULT_PRUNING_RATIO,
                it -> DEFAULT_PRUNING_RATIO = it
            );
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(NeuralSearchSettings.NEURAL_SPARSE_TWO_PHASE_MAX_WINDOW_SIZE, it -> MAX_WINDOW_SIZE = it);
    }

    public static NeuralSparseTwoPhaseParameters getDefaultSettings() {
        return new NeuralSparseTwoPhaseParameters().window_size_expansion(DEFAULT_WINDOW_SIZE_EXPANSION)
            .pruning_ratio(DEFAULT_PRUNING_RATIO)
            .enabled(DEFAULT_ENABLED);
    }

    /**
     * Constructor from stream input
     *
     * @param in StreamInput to initialize object from
     * @throws IOException thrown if unable to read from input stream
     */
    public NeuralSparseTwoPhaseParameters(StreamInput in) throws IOException {
        window_size_expansion = in.readFloat();
        pruning_ratio = in.readFloat();
        enabled = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeFloat(window_size_expansion);
        out.writeFloat(pruning_ratio);
        out.writeBoolean(enabled);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME.getPreferredName());
        builder.field(WINDOW_SIZE_EXPANSION.getPreferredName(), window_size_expansion);
        builder.field(PRUNING_RATIO.getPreferredName(), pruning_ratio);
        builder.field(ENABLED.getPreferredName(), enabled);
        builder.endObject();
        return builder;
    }

    public static NeuralSparseTwoPhaseParameters parseFromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token;
        String currentFieldName = "";
        NeuralSparseTwoPhaseParameters neuralSparseTwoPhaseParameters = NeuralSparseTwoPhaseParameters.getDefaultSettings();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (WINDOW_SIZE_EXPANSION.match(currentFieldName, parser.getDeprecationHandler())) {
                    neuralSparseTwoPhaseParameters.window_size_expansion(parser.floatValue());
                } else if (PRUNING_RATIO.match(currentFieldName, parser.getDeprecationHandler())) {
                    neuralSparseTwoPhaseParameters.pruning_ratio(parser.floatValue());
                } else if (ENABLED.match(currentFieldName, parser.getDeprecationHandler())) {
                    neuralSparseTwoPhaseParameters.enabled(parser.booleanValue());
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

    public int hashcode() {
        HashCodeBuilder builder = new HashCodeBuilder().append(enabled).append(window_size_expansion).append(pruning_ratio);
        return builder.toHashCode();
    }

    public static boolean isEnabled(NeuralSparseTwoPhaseParameters neuralSparseTwoPhaseParameters) {
        if (Objects.isNull(neuralSparseTwoPhaseParameters)) {
            return false;
        }
        return neuralSparseTwoPhaseParameters.enabled();
    }

    public static boolean isClusterOnOrAfterMinReqVersionForTwoPhaseSearchSupport() {
        return NeuralSearchClusterUtil.instance().getClusterMinVersion().onOrAfter(MINIMAL_SUPPORTED_VERSION_TWO_PHASE_SEARCH);
    }
}
