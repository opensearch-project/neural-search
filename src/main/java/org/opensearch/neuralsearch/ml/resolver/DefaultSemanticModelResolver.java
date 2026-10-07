/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.ml.resolver;

import lombok.NonNull;
import lombok.extern.log4j.Log4j2;
import org.opensearch.neuralsearch.mapper.dto.ModelSelection;
import org.opensearch.neuralsearch.settings.SemanticModelSelectionSettingsAccessor;

import java.util.Locale;

import static org.opensearch.neuralsearch.settings.NeuralSearchSettings.SEMANTIC_MODEL_SELECTION_MODEL_ID_PREFIX;

/**
 * Default {@link SemanticModelResolver} implementation. It resolves a {@link ModelSelection} to a model_id by reading
 * the operator configured {@code plugins.neural_search.model_selection.model_id.<model_type>.<language_option>} cluster
 * setting. It does not register, deploy, or fetch any model; the operator is expected to deploy the model and configure
 * the setting. Verification that the resolved model exists and its type matches the requested model type happens later,
 * where the model is loaded to build the semantic info field.
 */
@Log4j2
public class DefaultSemanticModelResolver implements SemanticModelResolver {

    private final SemanticModelSelectionSettingsAccessor settingsAccessor;

    public DefaultSemanticModelResolver(@NonNull final SemanticModelSelectionSettingsAccessor settingsAccessor) {
        this.settingsAccessor = settingsAccessor;
    }

    @Override
    public String resolve(@NonNull final ModelSelection modelSelection) {
        final String profileKey = modelSelection.getProfileKey();
        final String modelId = settingsAccessor.getModelId(profileKey);

        if (modelId == null) {
            final String settingKey = SEMANTIC_MODEL_SELECTION_MODEL_ID_PREFIX + profileKey;
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "No model_id is configured for the model_selection [language_option=%s, model_type=%s]. "
                        + "Deploy a %s model and set the cluster setting [%s] to its model_id, then retry.",
                    modelSelection.getLanguageOption(),
                    modelSelection.getModelType(),
                    modelSelection.getModelType(),
                    settingKey
                )
            );
        }

        return modelId;
    }
}
