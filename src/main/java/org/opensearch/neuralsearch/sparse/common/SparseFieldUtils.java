/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.common;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.neuralsearch.sparse.SparseSettings;
import org.opensearch.neuralsearch.sparse.mapper.SparseTokensFieldType;
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Utility class for operations related to sparse fields in neural search indices.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class SparseFieldUtils {

    /**
     * Retrieves all sparse ANN fields from a given index.
     *
     * @param index The name of the index
     * @return A set of field names that are configured as sparse token fields, or an empty set if none exist
     */
    @SuppressWarnings("unchecked")
    public static Set<String> getSparseAnnFields(String index) {
        if (index == null) {
            return Collections.emptySet();
        }
        final IndexMetadata metadata = NeuralSearchClusterUtil.instance().getClusterService().state().metadata().index(index);
        if (metadata == null || !SparseSettings.IS_SPARSE_INDEX_SETTING.get(metadata.getSettings())) {
            return Collections.emptySet();
        }
        MappingMetadata mappingMetadata = metadata.mapping();
        if (mappingMetadata == null || mappingMetadata.sourceAsMap() == null) {
            return Collections.emptySet();
        }
        Object properties = mappingMetadata.sourceAsMap().get("properties");
        if (!(properties instanceof Map)) {
            return Collections.emptySet();
        }
        Set<String> sparseAnnFields = new HashSet<>();
        Map<String, Object> fields = (Map<String, Object>) properties;
        for (Map.Entry<String, Object> field : fields.entrySet()) {
            Map<String, Object> fieldMap = (Map<String, Object>) field.getValue();
            Object type = fieldMap.get("type");
            if (Objects.nonNull(type) && SparseTokensFieldType.isSparseTokensType(type.toString())) {
                sparseAnnFields.add(field.getKey());
            }
        }
        return sparseAnnFields;
    }
}
