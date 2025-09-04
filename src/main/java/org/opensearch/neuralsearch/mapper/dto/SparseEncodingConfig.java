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
import org.opensearch.neuralsearch.util.prune.PruneType;
import org.opensearch.neuralsearch.util.prune.PruneUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.SPARSE_ENCODING_CONFIG;
import static org.opensearch.neuralsearch.util.prune.PruneUtils.PRUNE_RATIO_FIELD;
import static org.opensearch.neuralsearch.util.prune.PruneUtils.PRUNE_TYPE_FIELD;

@Getter
public class SparseEncodingConfig {
    private PruneType pruneType;
    private Float pruneRatio;

    /**
     * Construct SparseEncodingConfig with the json input defined in the sparse encoding config in the index mappings
     * @param name parameter name
     * @param value parameter value
     */
    public SparseEncodingConfig(@NonNull final String name, final Object value) {
        if (value instanceof Map == false) {
            throw new MapperParsingException(String.format(Locale.ROOT, "[%s] must be a Map", name));
        }
        final Map<String, Object> config = new HashMap<>((Map<String, Object>) value);
        final PruneType pruneType = consumePruneType(config);
        final Float pruneRatio = consumePruneRatio(config);

        // Check for any unrecognized parameters
        if (config.isEmpty() == false) {
            throw new MapperParsingException(
                String.format(Locale.ROOT, "Unsupported parameters %s in %s", String.join(",", config.keySet()), name)
            );
        }

        // Case: pruneType is null and pruneRatio is null → nothing configured
        if (pruneType == null && pruneRatio == null) {
            return;
        }

        // Case: pruneRatio is set but pruneType is null or NONE → invalid
        if (pruneRatio != null && (pruneType == null | PruneType.NONE.equals(pruneType))) {
            throw new MapperParsingException(
                String.format(
                    Locale.ROOT,
                    "%s should not be defined when %s is %s or null",
                    PRUNE_RATIO_FIELD,
                    PRUNE_TYPE_FIELD,
                    PruneType.NONE.getValue()
                )
            );
        }

        // Case: pruneType is defined and not NONE and pruneRatio is null → missing pruneRatio
        if (pruneRatio == null && PruneType.NONE.equals(pruneType) == false) {
            throw new MapperParsingException(
                String.format(
                    Locale.ROOT,
                    "%s is required when %s is defined and not %s",
                    PRUNE_RATIO_FIELD,
                    PRUNE_TYPE_FIELD,
                    PruneType.NONE.getValue()
                )
            );
        }

        // Case: pruneType is NONE and pruneRatio is null
        if (pruneRatio == null) {
            this.pruneType = pruneType;
            return;
        }

        // Case: pruneType is not NONE or null and pruneRatio is not null
        if (PruneUtils.isValidPruneRatio(pruneType, pruneRatio) == false) {
            throw new MapperParsingException(
                String.format(
                    Locale.ROOT,
                    "Illegal prune_ratio %f for prune_type: %s. %s",
                    pruneRatio,
                    pruneType.getValue(),
                    PruneUtils.getValidPruneRatioDescription(pruneType)
                )
            );
        }

        this.pruneType = pruneType;
        this.pruneRatio = pruneRatio;
    }

    /**
     * Construct SparseEncodingConfig with a valid semantic field. Only should be used with a valid semantic field config.
     * @param fieldConfig semantic field config
     */
    public SparseEncodingConfig(@NonNull final Map<String, Object> fieldConfig) {
        if (fieldConfig.containsKey(SPARSE_ENCODING_CONFIG) == false) {
            return;
        }
        final Map<String, Object> sparseEncodingConfig = (Map<String, Object>) fieldConfig.get(SPARSE_ENCODING_CONFIG);
        final PruneType pruneType = readPruneType(sparseEncodingConfig);
        final Float pruneRatio = readPruneRatio(sparseEncodingConfig);
        if (pruneType == null && pruneRatio == null) {
            return;
        }
        this.pruneType = pruneType;
        this.pruneRatio = pruneRatio;
    }

    private Float readPruneRatio(@NonNull final Map<String, Object> config) {
        if (config.containsKey(PRUNE_RATIO_FIELD)) {
            try {
                return Float.parseFloat(config.get(PRUNE_RATIO_FIELD).toString());
            } catch (Exception e) {
                throw new MapperParsingException(String.format(Locale.ROOT, "[%s] must be a Float", PRUNE_RATIO_FIELD));
            }
        }
        return null;
    }

    private Float consumePruneRatio(@NonNull final Map<String, Object> config) {
        try {
            return readPruneRatio(config);
        } finally {
            config.remove(PRUNE_RATIO_FIELD);
        }
    }

    private PruneType readPruneType(@NonNull final Map<String, Object> config) {
        if (config.containsKey(PRUNE_TYPE_FIELD)) {
            try {
                return PruneType.fromString((String) config.get(PRUNE_TYPE_FIELD));
            } catch (Exception e) {
                throw new MapperParsingException(
                    String.format(Locale.ROOT, "Invalid [%s]. Valid values are [%s].", PRUNE_TYPE_FIELD, PruneType.getValidValues())
                );
            }
        }
        return null;
    }

    private PruneType consumePruneType(@NonNull final Map<String, Object> config) {
        try {
            return readPruneType(config);
        } finally {
            config.remove(PRUNE_TYPE_FIELD);
        }
    }

    public void toXContent(@NonNull final XContentBuilder builder, String name) throws IOException {
        builder.startObject(name);
        if (pruneType != null) {
            builder.field(PRUNE_TYPE_FIELD, pruneType.getValue());
        }
        if (pruneRatio != null) {
            builder.field(PRUNE_RATIO_FIELD, pruneRatio.floatValue());
        }
        builder.endObject();
    }

    @Override
    public String toString() {
        final Map<String, Object> config = new HashMap<>();
        if (pruneType != null) {
            config.put(PRUNE_TYPE_FIELD, pruneType.getValue());
        }
        if (pruneRatio != null) {
            config.put(PRUNE_RATIO_FIELD, pruneRatio);
        }
        return config.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj != null && this.getClass() == obj.getClass()) {
            SparseEncodingConfig other = (SparseEncodingConfig) obj;
            EqualsBuilder equalsBuilder = new EqualsBuilder();
            equalsBuilder.append(this.pruneType, other.pruneType);
            equalsBuilder.append(this.pruneRatio, other.pruneRatio);
            return equalsBuilder.isEquals();
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return (new HashCodeBuilder()).append(this.pruneType).append(this.pruneRatio).toHashCode();
    }
}
