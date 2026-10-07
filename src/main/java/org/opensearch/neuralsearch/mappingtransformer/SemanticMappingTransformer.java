/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.mappingtransformer;

import com.google.common.annotations.VisibleForTesting;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.mapper.MappingTransformer;

import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.MLModel;
import org.opensearch.neuralsearch.constants.SemanticFieldConstants;
import org.opensearch.neuralsearch.mapper.dto.ModelSelection;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.ml.resolver.SemanticModelResolver;
import org.opensearch.neuralsearch.util.SemanticMLModelUtils;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.neuralsearch.constants.MappingConstants.PROPERTIES;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.MODEL_ID;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.MODEL_SELECTION;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.SEMANTIC_INFO_FIELD_NAME;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.SPARSE_ENCODING_CONFIG;
import static org.opensearch.neuralsearch.util.SemanticMappingUtils.collectSemanticField;
import static org.opensearch.neuralsearch.util.SemanticMappingUtils.extractModelIdToFieldPathMap;
import static org.opensearch.neuralsearch.util.SemanticMappingUtils.getDenseEmbeddingConfig;
import static org.opensearch.neuralsearch.util.SemanticMappingUtils.isChunkingEnabled;
import static org.opensearch.neuralsearch.util.SemanticMappingUtils.getSemanticFieldSearchAnalyzer;
import static org.opensearch.neuralsearch.util.SemanticMappingUtils.getProperties;
import static org.opensearch.neuralsearch.util.SemanticMappingUtils.validateModelId;
import static org.opensearch.neuralsearch.util.SemanticMappingUtils.validateSemanticInfoFieldName;

/**
 * SemanticMappingTransformer transforms the index mapping for the semantic field to auto add the semantic info fields
 * based on the ML model id defined in the semantic field.
 * <p>
 * This transformer is the single enforcement point for the {@code model_id}/{@code model_selection} rules. The
 * {@link org.opensearch.neuralsearch.mapper.SemanticFieldMapper} TypeParser intentionally accepts both together (so a
 * GET mapping response, which contains the resolved {@code model_id} alongside {@code model_selection}, can be reapplied
 * via PUT). It resolves {@code model_selection} to a {@code model_id}, and, when an explicit {@code model_id} is also
 * supplied, verifies the two point to the same model. The mapper cannot perform the check itself as it has no access to
 * the cluster-setting resolution at parse time.
 * <p>
 * The mapping transformers are invoked by the core {@code MappingTransformerRegistry} from all mapping-defining
 * transport actions, so this transformer runs on the raw mapping for every path that can introduce or update a semantic
 * field:
 * <ul>
 *   <li>{@code TransportCreateIndexAction} (create index)</li>
 *   <li>{@code TransportPutMappingAction} (update mapping)</li>
 *   <li>{@code TransportPutIndexTemplateAction} (legacy index template)</li>
 *   <li>{@code TransportPutComponentTemplateAction} (component template)</li>
 *   <li>{@code TransportPutComposableIndexTemplateAction} (composable index template)</li>
 * </ul>
 * Note that for the composable index template path the transformed mapping is persisted into the stored template, so a
 * {@code model_selection} in a template is resolved to a concrete {@code model_id} at template-creation time.
 */
@Log4j2
public class SemanticMappingTransformer implements MappingTransformer {
    public final static Set<String> SUPPORTED_MODEL_ALGORITHMS = Set.of(
        FunctionName.TEXT_EMBEDDING.name(),
        FunctionName.REMOTE.name(),
        FunctionName.SPARSE_ENCODING.name(),
        FunctionName.SPARSE_TOKENIZE.name()
    );
    public final static Set<String> SUPPORTED_REMOTE_MODEL_TYPES = Set.of(
        FunctionName.TEXT_EMBEDDING.name(),
        FunctionName.SPARSE_ENCODING.name(),
        FunctionName.SPARSE_TOKENIZE.name()
    );

    private final MLCommonsClientAccessor mlClientAccessor;
    private final NamedXContentRegistry xContentRegistry;

    @Setter
    private volatile SemanticModelResolver modelResolver;

    public SemanticMappingTransformer(final MLCommonsClientAccessor mlClientAccessor, final NamedXContentRegistry xContentRegistry) {
        this.mlClientAccessor = mlClientAccessor;
        this.xContentRegistry = xContentRegistry;
    }

    /**
     * Add semantic info fields to the mapping.
     * @param mapping original mapping
     * e.g.
     *{
     *   "_doc": {
     *     "properties": {
     *       "semantic_field": {
     *         "model_id": "model_id",
     *         "type": "semantic"
     *       }
     *     }
     *   }
     * }
     *
     * It can be transformed to
     *{
     *   "_doc": {
     *     "properties": {
     *       "semantic_field": {
     *         "model_id": "model_id",
     *         "type": "semantic"
     *       },
     *       "semantic_field_semantic_info": {
     *         "properties": {
     *           "chunks": {
     *             "type": "nested",
     *             "properties": {
     *               "embedding": {
     *                 "type": "knn_vector",
     *                 "dimension": 768,
     *                 "method": {
     *                   "engine": "faiss",
     *                   "space_type": "l2",
     *                   "name": "hnsw",
     *                   "parameters": {}
     *                 }
     *               },
     *               "text": {
     *                 "type": "text"
     *               }
     *             }
     *           },
     *           "model": {
     *             "properties": {
     *               "id": {
     *                 "type": "text",
     *                 "index": false
     *               },
     *               "name": {
     *                 "type": "text",
     *                 "index": false
     *               },
     *               "type": {
     *                 "type": "text",
     *                 "index": false
     *               }
     *             }
     *           }
     *         }
     *       }
     *     }
     *   }
     * }
     * @param context context to help transform
     */

    @Override
    public void transform(final Map<String, Object> mapping, final TransformContext context, @NonNull final ActionListener<Void> listener) {
        try {
            final Map<String, Object> properties = getProperties(mapping);
            // If there is no property or its format is not valid we simply do nothing and rely on core to validate the
            // mappings and handle the error.
            if (properties.isEmpty()) {
                listener.onResponse(null);
                return;
            }

            final Map<String, Map<String, Object>> semanticFieldPathToConfigMap = new HashMap<>();

            collectSemanticField(properties, semanticFieldPathToConfigMap);

            // Split fields into those that carry a model_selection (need model_id resolution from the cluster settings)
            // and those that rely on an explicit model_id (existing path). Fields with neither model_selection nor
            // model_id are kept in fieldsWithModelId so the existing validation surfaces the missing model_id error.
            // Use a LinkedHashMap for the fields to resolve so resolution order is deterministic.
            final Map<String, Map<String, Object>> fieldsToResolve = new LinkedHashMap<>();
            final Map<String, Map<String, Object>> fieldsWithModelId = new HashMap<>();

            for (Map.Entry<String, Map<String, Object>> entry : semanticFieldPathToConfigMap.entrySet()) {
                final Map<String, Object> config = entry.getValue();
                if (config.get(MODEL_SELECTION) != null) {
                    fieldsToResolve.put(entry.getKey(), config);
                } else {
                    fieldsWithModelId.put(entry.getKey(), config);
                }
            }

            validateSemanticFields(fieldsWithModelId);

            // Resolve model_selection fields (a cheap, synchronous cluster-setting lookup) and record the requested
            // ModelSelection per field so the model type can be verified later, when the model is loaded.
            final Map<String, ModelSelection> modelSelectionByPath = new HashMap<>();
            if (fieldsToResolve.isEmpty() == false) {
                if (modelResolver == null) {
                    throw new IllegalStateException(
                        "Cannot resolve the model_selection because the SemanticModelResolver is not configured."
                    );
                }
                for (final Map.Entry<String, Map<String, Object>> entry : fieldsToResolve.entrySet()) {
                    resolveModelSelectionField(entry.getKey(), entry.getValue(), fieldsWithModelId, modelSelectionByPath);
                }
            }

            if (fieldsWithModelId.isEmpty()) {
                listener.onResponse(null);
            } else {
                fetchModelAndModifyMapping(fieldsWithModelId, modelSelectionByPath, properties, listener);
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }

    }

    private void resolveModelSelectionField(
        @NonNull final String fieldPath,
        @NonNull final Map<String, Object> fieldConfig,
        @NonNull final Map<String, Map<String, Object>> fieldsWithModelId,
        @NonNull final Map<String, ModelSelection> modelSelectionByPath
    ) {
        final ModelSelection modelSelection = new ModelSelection(MODEL_SELECTION, fieldConfig.get(MODEL_SELECTION));
        final String resolvedModelId = modelResolver.resolve(modelSelection);
        final Object suppliedModelId = fieldConfig.get(MODEL_ID);

        // Log the resolution so drift is diagnosable. On an update that re-applies a model_selection field, this line
        // records which model the field resolved to; if the resolved id differs from what an existing field was bound
        // to, comparing successive log lines surfaces that the field moved off its previous model.
        log.info(
            "Resolved model_selection [language_option={}, model_type={}] to model_id [{}] for semantic field at [{}].",
            modelSelection.getLanguageOption(),
            modelSelection.getModelType(),
            resolvedModelId,
            fieldPath
        );

        if (suppliedModelId != null) {
            if (suppliedModelId instanceof String == false) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "The model_id for the semantic field at %s must be a string.", fieldPath)
                );
            }
            // When both model_id and model_selection are provided, they must point to the same model.
            if (resolvedModelId.equals(suppliedModelId) == false) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "The provided model_id [%s] for the semantic field at %s does not match the model_id [%s] resolved "
                            + "from model_selection [language_option=%s, model_type=%s]. Provide the matching model_id or omit it.",
                        suppliedModelId,
                        fieldPath,
                        resolvedModelId,
                        modelSelection.getLanguageOption(),
                        modelSelection.getModelType()
                    )
                );
            }
        } else {
            fieldConfig.put(MODEL_ID, resolvedModelId);
        }

        // Validate the resolved field consistently with the explicit model_id path (e.g. semantic_info_field_name
        // format). Model selection fields skip the upfront validateSemanticFields call, so validate them here.
        validateSemanticFields(Map.of(fieldPath, fieldConfig));

        fieldsWithModelId.put(fieldPath, fieldConfig);
        modelSelectionByPath.put(fieldPath, modelSelection);
    }

    private void validateSemanticFields(@NonNull final Map<String, Map<String, Object>> semanticFieldPathToConfigMap) {
        final List<String> errors = new ArrayList<>();
        for (Map.Entry<String, Map<String, Object>> entry : semanticFieldPathToConfigMap.entrySet()) {
            final String semanticFieldPath = entry.getKey();
            final Map<String, Object> semanticFieldConfig = entry.getValue();
            final String validateModelIdError = validateModelId(semanticFieldPath, semanticFieldConfig);
            final String validateSemanticInfoFieldNameError = validateSemanticInfoFieldName(semanticFieldPath, semanticFieldConfig);
            if (validateModelIdError != null) {
                errors.add(validateModelIdError);
            }
            if (validateSemanticInfoFieldNameError != null) {
                errors.add(validateSemanticInfoFieldNameError);
            }
        }
        if (errors.isEmpty() == false) {
            throw new IllegalArgumentException(String.join("; ", errors));
        }
    }

    private void fetchModelAndModifyMapping(
        @NonNull final Map<String, Map<String, Object>> semanticFieldPathToConfigMap,
        @NonNull final Map<String, ModelSelection> modelSelectionByPath,
        @NonNull final Map<String, Object> mappings,
        @NonNull final ActionListener<Void> listener
    ) {
        final Map<String, List<String>> modelIdToFieldPathMap = extractModelIdToFieldPathMap(semanticFieldPathToConfigMap);

        mlClientAccessor.getModels(modelIdToFieldPathMap.keySet(), modelIdToConfigMap -> {
            modifyMappings(modelIdToConfigMap, mappings, modelIdToFieldPathMap, semanticFieldPathToConfigMap, modelSelectionByPath);
            listener.onResponse(null);
        }, listener::onFailure);
    }

    private void modifyMappings(
        @NonNull final Map<String, MLModel> modelIdToConfigMap,
        @NonNull final Map<String, Object> mappings,
        @NonNull final Map<String, List<String>> modelIdToFieldPathMap,
        @NonNull final Map<String, Map<String, Object>> semanticFieldPathToConfigMap,
        @NonNull final Map<String, ModelSelection> modelSelectionByPath
    ) {
        for (String modelId : modelIdToFieldPathMap.keySet()) {
            final MLModel mlModel = modelIdToConfigMap.get(modelId);
            final List<String> fieldPathList = modelIdToFieldPathMap.get(modelId);
            for (String fieldPath : fieldPathList) {
                try {
                    final Map<String, Object> fieldConfig = semanticFieldPathToConfigMap.get(fieldPath);
                    // For fields resolved from a model_selection, verify the resolved model's type matches the
                    // requested model_type now that the model is loaded (single fetch, no separate resolve-time fetch).
                    final ModelSelection modelSelection = modelSelectionByPath.get(fieldPath);
                    if (modelSelection != null) {
                        validateModelTypeMatches(modelSelection, mlModel, modelId);
                    }
                    final Map<String, Object> semanticInfoConfig = createSemanticInfoField(mlModel, modelId, fieldConfig, fieldPath);
                    setSemanticInfoField(mappings, fieldPath, fieldConfig.get(SEMANTIC_INFO_FIELD_NAME), semanticInfoConfig);
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException(getModifyMappingErrorMessage(fieldPath, e.getMessage()), e);
                } catch (Exception e) {
                    throw new RuntimeException(getModifyMappingErrorMessage(fieldPath, e.getMessage()), e);
                }
            }
        }
    }

    private void validateModelTypeMatches(
        @NonNull final ModelSelection modelSelection,
        @NonNull final MLModel mlModel,
        @NonNull final String modelId
    ) {
        final boolean modelIsDense = SemanticMLModelUtils.isDenseModel(SemanticMLModelUtils.getModelType(mlModel));
        if (modelIsDense != modelSelection.isDense()) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "The model [%s] is a %s model, which does not match the requested model_type [%s].",
                    modelId,
                    modelIsDense ? ModelSelection.DENSE : ModelSelection.SPARSE,
                    modelSelection.getModelType()
                )
            );
        }
    }

    private String getModifyMappingErrorMessage(@NonNull final String fieldPath, final String cause) {
        return String.format(Locale.ROOT, "Failed to transform the mapping for the semantic field at %s due to %s", fieldPath, cause);
    }

    @VisibleForTesting
    private Map<String, Object> createSemanticInfoField(
        final @NonNull MLModel modelConfig,
        final String modelId,
        @NonNull final Map<String, Object> fieldConfig,
        String fieldPath
    ) {
        final SemanticInfoConfigBuilder builder = new SemanticInfoConfigBuilder(xContentRegistry);
        builder.mlModel(modelConfig, modelId);
        builder.chunkingEnabled(isChunkingEnabled(fieldConfig, fieldPath));
        builder.semanticFieldSearchAnalyzer(getSemanticFieldSearchAnalyzer(fieldConfig, fieldPath));
        builder.denseEmbeddingConfig(getDenseEmbeddingConfig(fieldConfig, fieldPath));
        builder.sparseEncodingConfigDefined(fieldConfig.containsKey(SPARSE_ENCODING_CONFIG));
        return builder.build();
    }

    @SuppressWarnings("unchecked")
    private void setSemanticInfoField(
        @NonNull final Map<String, Object> mappings,
        @NonNull final String fullPath,
        final Object userDefinedSemanticInfoFieldName,
        @NonNull final Map<String, Object> semanticInfoConfig
    ) {
        Map<String, Object> current = mappings;
        final String[] paths = fullPath.split("\\.");
        final String semanticInfoFieldName = userDefinedSemanticInfoFieldName == null
            ? paths[paths.length - 1] + SemanticFieldConstants.DEFAULT_SEMANTIC_INFO_FIELD_NAME_SUFFIX
            : (String) userDefinedSemanticInfoFieldName;

        for (int i = 0; i < paths.length - 1; i++) {
            String interFieldName = paths[i];
            Map<String, Object> interFieldConfig = (Map<String, Object>) current.get(interFieldName);

            // In OpenSearch we allow users to use "." in the field name when they define the index mapping. OpenSearch
            // core wll unflatten it later in the mapper service but here we need to unflatten the path to the semantic
            // field so that we can set the semantic info fields config by the path.
            if (interFieldConfig == null) {
                interFieldConfig = unflattenMapping(interFieldName, current);
            }

            // handle the case when the inter field is an object field
            if (interFieldConfig.containsKey(PROPERTIES)) {
                current = (Map<String, Object>) interFieldConfig.get(PROPERTIES);
            }
        }

        // We simply set the whole semantic info config at the path of the semantic info. It is possible the config of
        // semantic info fields can be invalid, but we will not validate it here. We will rely on the field mappers to
        // validate them when they parse the mappings.
        current.put(semanticInfoFieldName, semanticInfoConfig);
    }

    /**
     * e.g. input:
     * interFieldName: description
     * current mapping:
     * {
     *   "description.test1": {
     *         "type": "text"
     *    }
     *    "description.test2": {
     *          "type": "text"
     *    }
     * }
     * then output:
     * {
     *     test1": {
     *         "type": "text"
     *     },
     *     test2": {
     *         "type": "text"
     *     }
     * }
     *
     * @param interFieldName inter field name
     * @param current current mapping
     * @return unflattened inter field config
     */
    private Map<String, Object> unflattenMapping(@NonNull final String interFieldName, @NonNull final Map<String, Object> current) {
        final String prefix = interFieldName + ".";
        final Map<String, Object> properties = new HashMap<>();
        final Map<String, Object> interFieldConfig = new HashMap<>();
        interFieldConfig.put(PROPERTIES, properties);
        final Set<String> matchedKeySet = current.keySet().stream().filter(k -> k.startsWith(prefix)).collect(Collectors.toSet());
        for (String key : matchedKeySet) {
            properties.put(key.substring(prefix.length()), current.get(key));
        }
        matchedKeySet.forEach(current::remove);
        current.put(interFieldName, interFieldConfig);
        return interFieldConfig;
    }
}
