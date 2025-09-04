/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.mapper.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.mapper.MapperParsingException;
import org.opensearch.neuralsearch.processor.chunker.ChunkerValidatorFactory;
import org.opensearch.neuralsearch.processor.chunker.Validator;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.CHUNKING;
import static org.opensearch.neuralsearch.processor.chunker.ChunkerFactory.CHUNKER_ALGORITHMS;

@Builder
@AllArgsConstructor
@Getter
public class ChunkingConfig {
    /**
     * algorithm parameter name
     */
    public static final String ALGORITHM_FIELD = "algorithm";
    /**
     * parameters parameter name
     */
    public static final String PARAMETERS_FIELD = "parameters";
    private List<Map<String, Object>> configs;
    private boolean enabled;

    /**
     * Construct ChunkingConfig with the json input defined in the chunking config of the semantic field in the index
     * mappings.
     * @param name parameter name
     * @param value parameter value
     */
    public ChunkingConfig(@NonNull final String name, final Object value, @NonNull final ChunkerValidatorFactory chunkerValidatorFactory) {
        if (value instanceof Boolean) {
            this.enabled = (Boolean) value;
            return;
        }

        if (value instanceof List<?>) {
            List<Object> configList = (List<Object>) value;
            if (configList.stream().allMatch(item -> item instanceof Map<?, ?>)) {
                List<Map<String, Object>> parsedConfigs = configList.stream().map(item -> {
                    validate((Map<String, Object>) item, chunkerValidatorFactory);
                    return (Map<String, Object>) item;
                }).collect(Collectors.toList());

                this.enabled = true;
                this.configs = parsedConfigs;
                return;
            }
        }

        throw new MapperParsingException(String.format(Locale.ROOT, "[%s] must be either a List of Maps or a Boolean", name));
    }

    /**
     * Construct ChunkingConfig with valida semantic field config.
     * @param fieldConfig semantic field config
     */
    public ChunkingConfig(@NonNull final Map<String, Object> fieldConfig) {
        if (fieldConfig.containsKey(CHUNKING) == false) {
            return;
        }
        final Object chunkingConfig = fieldConfig.get(CHUNKING);
        if (chunkingConfig instanceof Boolean) {
            this.enabled = (Boolean) chunkingConfig;
            return;
        }
        if (chunkingConfig instanceof List<?>) {
            List<Map<String, Object>> configList = (List<Map<String, Object>>) chunkingConfig;
            this.configs = configList;
            this.enabled = true;
            return;
        }
        throw new IllegalStateException(
            String.format(Locale.ROOT, "Invalid %s in the semantic field config. It is neither a Boolean nor a List of Maps.", CHUNKING)
        );
    }

    private void validate(@NonNull final Map<String, Object> item, @NonNull final ChunkerValidatorFactory chunkerValidatorFactory) {
        validateUnsupportedParameters(item);
        final String algorithm = readAlgorithm(item);
        if (item.get(PARAMETERS_FIELD) instanceof Map<?, ?>) {
            Validator validator = chunkerValidatorFactory.getValidator(algorithm);
            if (validator == null) {
                throw new MapperParsingException(
                    String.format(
                        Locale.ROOT,
                        "[%s] is not a valid chunking algorithm. A valid algorithm should be %s.",
                        algorithm,
                        CHUNKER_ALGORITHMS
                    )
                );
            }
            validator.validate((Map<String, Object>) item.get(PARAMETERS_FIELD));
        } else if (item.get(PARAMETERS_FIELD) != null) {
            throw new MapperParsingException(String.format(Locale.ROOT, "%s in %s should be a Map.", PARAMETERS_FIELD, CHUNKING));
        }
    }

    private void validateUnsupportedParameters(@NonNull Map<String, Object> item) {
        final Set<String> keys = new HashSet<>(item.keySet());
        keys.remove(ALGORITHM_FIELD);
        keys.remove(PARAMETERS_FIELD);
        if (!keys.isEmpty()) {
            throw new MapperParsingException(String.format(Locale.ROOT, "[%s] does not support parameters [%s]", CHUNKING, keys));
        }
    }

    private String readAlgorithm(Map<String, Object> config) {
        if (config.containsKey(ALGORITHM_FIELD)) {
            if (config.get(ALGORITHM_FIELD) instanceof String algorithm && CHUNKER_ALGORITHMS.contains(algorithm)) {
                return algorithm;
            }
            throw new MapperParsingException(String.format(Locale.ROOT, "[%s] must be %s", ALGORITHM_FIELD, CHUNKER_ALGORITHMS));
        }
        throw new MapperParsingException(
            String.format(Locale.ROOT, "[%s] is required to configure the chunking strategy.", ALGORITHM_FIELD)
        );
    }

    public void toXContent(@NonNull final XContentBuilder builder, String name) throws IOException {
        if (configs == null) {
            builder.field(name, enabled);
        } else {
            builder.startArray(name);
            for (Map<String, Object> config : configs) {
                builder.map(config);
            }
            builder.endArray();
        }
    }

    @Override
    public String toString() {
        if (configs == null) {
            return String.valueOf(enabled);
        } else {
            return configs.toString();
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;

        ChunkingConfig other = (ChunkingConfig) obj;
        return new EqualsBuilder().append(enabled, other.enabled).append(configs, other.configs).isEquals();
    }

    @Override
    public int hashCode() {
        return (new HashCodeBuilder()).append(this.enabled).append(this.configs).toHashCode();
    }
}
