/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.mapper.dto;

import lombok.Getter;
import lombok.NonNull;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.mapper.MapperParsingException;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.LANGUAGE_OPTION;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.MODEL_TYPE;

/**
 * DTO for the {@code model_selection} nested object of a semantic field. It holds the human readable
 * description of the model the customer wants (language + model type) which the system resolves to a
 * concrete {@code model_id} through the {@code plugins.neural_search.model_selection.model_id.*} cluster
 * settings.
 * <p>
 * Example:
 * <pre>
 * "model_selection": {
 *   "language_option": "ENGLISH",
 *   "model_type": "SPARSE"
 * }
 * </pre>
 */
@Getter
public class ModelSelection {
    public static final String ENGLISH = "ENGLISH";
    public static final String MULTILINGUAL = "MULTILINGUAL";
    public static final String SPARSE = "SPARSE";
    public static final String DENSE = "DENSE";

    public static final Set<String> SUPPORTED_LANGUAGE_OPTIONS = Set.of(ENGLISH, MULTILINGUAL);
    public static final Set<String> SUPPORTED_MODEL_TYPES = Set.of(SPARSE, DENSE);

    private final String languageOption;
    private final String modelType;

    /**
     * Construct a ModelSelection from the raw value defined under {@code model_selection} in the index mappings.
     * Missing sub-fields default to ENGLISH / SPARSE.
     *
     * @param name  parameter name (used for error messages)
     * @param value raw parameter value, expected to be a Map
     */
    @SuppressWarnings("unchecked")
    public ModelSelection(@NonNull final String name, final Object value) {
        if (value instanceof Map == false) {
            throw new MapperParsingException(String.format(Locale.ROOT, "[%s] must be a Map", name));
        }
        final Map<String, Object> config = (Map<String, Object>) value;

        for (final String key : config.keySet()) {
            if (LANGUAGE_OPTION.equals(key) == false && MODEL_TYPE.equals(key) == false) {
                throw new MapperParsingException(String.format(Locale.ROOT, "Unsupported parameter [%s] in [%s]", key, name));
            }
        }

        this.languageOption = normalize(config.get(LANGUAGE_OPTION), LANGUAGE_OPTION, ENGLISH, SUPPORTED_LANGUAGE_OPTIONS);
        this.modelType = normalize(config.get(MODEL_TYPE), MODEL_TYPE, SPARSE, SUPPORTED_MODEL_TYPES);
    }

    /**
     * Construct a ModelSelection directly from its language option and model type.
     */
    public ModelSelection(final String languageOption, final String modelType) {
        this.languageOption = normalize(languageOption, LANGUAGE_OPTION, ENGLISH, SUPPORTED_LANGUAGE_OPTIONS);
        this.modelType = normalize(modelType, MODEL_TYPE, SPARSE, SUPPORTED_MODEL_TYPES);
    }

    private static String normalize(final Object rawValue, final String field, final String defaultValue, final Set<String> supported) {
        if (rawValue == null) {
            return defaultValue;
        }
        final String normalized = rawValue.toString().toUpperCase(Locale.ROOT);
        if (supported.contains(normalized) == false) {
            throw new MapperParsingException(
                String.format(
                    Locale.ROOT,
                    "Unsupported [%s] value [%s]. It should be one of [%s].",
                    field,
                    rawValue,
                    String.join(", ", supported)
                )
            );
        }
        return normalized;
    }

    /**
     * @return whether the customer requested a dense model.
     */
    public boolean isDense() {
        return DENSE.equals(modelType);
    }

    /**
     * @return the profile key used to look up the resolved model_id from the cluster settings. It is the suffix of the
     * affix setting {@code plugins.neural_search.model_selection.model_id.} and takes the form {@code <model_type>.<language_option>}
     * (both lower cased), e.g. {@code sparse.english}.
     */
    public String getProfileKey() {
        return modelType.toLowerCase(Locale.ROOT) + "." + languageOption.toLowerCase(Locale.ROOT);
    }

    public void toXContent(@NonNull final XContentBuilder builder, final String name) throws IOException {
        builder.startObject(name);
        builder.field(LANGUAGE_OPTION, languageOption);
        builder.field(MODEL_TYPE, modelType);
        builder.endObject();
    }

    @Override
    public String toString() {
        return String.format(Locale.ROOT, "{%s=%s, %s=%s}", LANGUAGE_OPTION, languageOption, MODEL_TYPE, modelType);
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        } else if (obj != null && this.getClass() == obj.getClass()) {
            final ModelSelection other = (ModelSelection) obj;
            return new EqualsBuilder().append(this.languageOption, other.languageOption).append(this.modelType, other.modelType).isEquals();
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(this.languageOption).append(this.modelType).toHashCode();
    }
}
