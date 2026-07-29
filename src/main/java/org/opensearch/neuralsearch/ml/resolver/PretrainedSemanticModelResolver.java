/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.ml.resolver;

import lombok.extern.log4j.Log4j2;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.model.MLModelFormat;
import org.opensearch.ml.common.model.MLModelState;
import org.opensearch.ml.common.transport.register.MLRegisterModelInput;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.transport.client.Client;

import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Default OSS implementation of SemanticModelResolver that resolves pretrained models
 * based on language option and model type combinations. It uses an in-memory cache,
 * searches the .plugins-ml-model index for existing models, and registers+deploys
 * new models if not found.
 */
@Log4j2
public class PretrainedSemanticModelResolver implements SemanticModelResolver {

    private static final String ML_MODEL_INDEX = ".plugins-ml-model";
    private static final String MODEL_NAME_KEYWORD_FIELD = "name.keyword";
    private static final String MODEL_ALGORITHM_FIELD = "algorithm";
    private static final String MODEL_VERSION_FIELD = "model_version";

    private static final Set<String> VALID_LANGUAGE_OPTIONS = Set.of("ENGLISH", "MULTILINGUAL");
    private static final Set<String> VALID_MODEL_TYPES = Set.of("SPARSE", "DENSE");

    private final MachineLearningNodeClient mlClient;
    private final Client client;
    private final ConcurrentHashMap<String, String> modelCache = new ConcurrentHashMap<>();

    /**
     * Model resolution table entry containing all metadata needed to register a pretrained model.
     */
    private record ModelSpec(String name, String version, FunctionName algorithm) {
    }

    private static final Map<String, ModelSpec> MODEL_RESOLUTION_TABLE = Map.of(
        "ENGLISH_SPARSE",
        new ModelSpec("amazon/neural-sparse/opensearch-neural-sparse-encoding-doc-v2-mini", "1.0.0", FunctionName.SPARSE_ENCODING),
        "ENGLISH_DENSE",
        new ModelSpec("huggingface/sentence-transformers/paraphrase-MiniLM-L3-v2", "1.0.1", FunctionName.TEXT_EMBEDDING),
        "MULTILINGUAL_SPARSE",
        new ModelSpec("amazon/neural-sparse/opensearch-neural-sparse-encoding-multilingual-v1", "1.0.1", FunctionName.SPARSE_ENCODING),
        "MULTILINGUAL_DENSE",
        new ModelSpec("huggingface/sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2", "1.0.2", FunctionName.TEXT_EMBEDDING)
    );

    public PretrainedSemanticModelResolver(MachineLearningNodeClient mlClient, Client client) {
        this.mlClient = mlClient;
        this.client = client;
    }

    @Override
    public void validate(String languageOption, String modelType) {
        if (languageOption != null && !VALID_LANGUAGE_OPTIONS.contains(languageOption.toUpperCase(Locale.ROOT))) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Unknown language_option [%s]. Supported values are: %s", languageOption, VALID_LANGUAGE_OPTIONS)
            );
        }
        if (modelType != null && !VALID_MODEL_TYPES.contains(modelType.toUpperCase(Locale.ROOT))) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Unknown model_type [%s]. Supported values are: %s", modelType, VALID_MODEL_TYPES)
            );
        }
    }

    @Override
    public void resolve(String languageOption, String modelType, ActionListener<String> listener) {
        final String normalizedLanguage = languageOption != null ? languageOption.toUpperCase(Locale.ROOT) : "ENGLISH";
        final String normalizedModelType = modelType != null ? modelType.toUpperCase(Locale.ROOT) : "SPARSE";

        validate(normalizedLanguage, normalizedModelType);

        final String cacheKey = normalizedLanguage + "_" + normalizedModelType;
        final ModelSpec spec = MODEL_RESOLUTION_TABLE.get(cacheKey);
        if (spec == null) {
            listener.onFailure(
                new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "No pretrained model available for language_option [%s] and model_type [%s]",
                        normalizedLanguage,
                        normalizedModelType
                    )
                )
            );
            return;
        }

        // Check in-memory cache first
        String cachedModelId = modelCache.get(cacheKey);
        if (cachedModelId != null) {
            listener.onResponse(cachedModelId);
            return;
        }

        // Search the ML model index for an existing model
        searchForExistingModel(spec, cacheKey, listener);
    }

    private void searchForExistingModel(ModelSpec spec, String cacheKey, ActionListener<String> listener) {
        BoolQueryBuilder query = QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery(MODEL_NAME_KEYWORD_FIELD, spec.name()))
            .must(QueryBuilders.termQuery(MODEL_ALGORITHM_FIELD, spec.algorithm().name()));

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(query).size(1);
        SearchRequest searchRequest = new SearchRequest(ML_MODEL_INDEX).source(sourceBuilder);

        try (ThreadContext.StoredContext ignored = client.threadPool().getThreadContext().stashContext()) {
            client.search(
                searchRequest,
                ActionListener.wrap(searchResponse -> { handleSearchResponse(searchResponse, spec, cacheKey, listener); }, e -> {
                    if (e instanceof IndexNotFoundException) {
                        log.info("ML model index not found, will register new model: {}", spec.name());
                        registerAndDeployModel(spec, cacheKey, listener);
                    } else {
                        log.warn("Error searching for existing model [{}], falling through to register", spec.name(), e);
                        registerAndDeployModel(spec, cacheKey, listener);
                    }
                })
            );
        }
    }

    private void handleSearchResponse(SearchResponse searchResponse, ModelSpec spec, String cacheKey, ActionListener<String> listener) {
        SearchHit[] hits = searchResponse.getHits().getHits();
        if (hits.length > 0) {
            String modelId = hits[0].getId();
            // Verify the model is deployed by fetching its info
            mlClient.getModel(modelId, null, ActionListener.wrap(mlModel -> {
                MLModelState state = mlModel.getModelState();
                if (state == MLModelState.DEPLOYED || state == MLModelState.LOADED) {
                    modelCache.put(cacheKey, modelId);
                    listener.onResponse(modelId);
                } else {
                    // Model exists but is not deployed; register a fresh one
                    log.info("Found model [{}] with id [{}] but state is [{}], registering new model", spec.name(), modelId, state);
                    registerAndDeployModel(spec, cacheKey, listener);
                }
            }, e -> {
                log.warn("Failed to get model [{}], registering new one", modelId, e);
                registerAndDeployModel(spec, cacheKey, listener);
            }));
        } else {
            registerAndDeployModel(spec, cacheKey, listener);
        }
    }

    private void registerAndDeployModel(ModelSpec spec, String cacheKey, ActionListener<String> listener) {
        MLRegisterModelInput registerInput = MLRegisterModelInput.builder()
            .modelName(spec.name())
            .version(spec.version())
            .modelFormat(MLModelFormat.TORCH_SCRIPT)
            .functionName(spec.algorithm())
            .deployModel(true)
            .build();

        mlClient.register(registerInput, ActionListener.wrap(registerResponse -> {
            String taskId = registerResponse.getTaskId();
            String modelId = registerResponse.getModelId();
            if (modelId != null && !modelId.isEmpty()) {
                modelCache.put(cacheKey, modelId);
                listener.onResponse(modelId);
                return;
            }
            if (taskId != null && !taskId.isEmpty()) {
                pollTaskForModelId(taskId, cacheKey, 0, listener);
            } else {
                listener.onFailure(new RuntimeException("Model registration did not return a task_id or model_id"));
            }
        }, listener::onFailure));
    }

    private static final int MAX_POLL_RETRIES = 60;
    private static final long POLL_INTERVAL_MS = 2000;

    private void pollTaskForModelId(String taskId, String cacheKey, int attempt, ActionListener<String> listener) {
        if (attempt >= MAX_POLL_RETRIES) {
            listener.onFailure(new RuntimeException("Timed out waiting for model registration task [" + taskId + "] to complete"));
            return;
        }

        mlClient.getTask(taskId, ActionListener.wrap(mlTask -> {
            String modelId = mlTask.getModelId();
            if (modelId != null && !modelId.isEmpty()) {
                modelCache.put(cacheKey, modelId);
                listener.onResponse(modelId);
            } else {
                // Task still in progress, schedule a retry
                client.threadPool().generic().execute(() -> {
                    try {
                        Thread.sleep(POLL_INTERVAL_MS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        listener.onFailure(e);
                        return;
                    }
                    pollTaskForModelId(taskId, cacheKey, attempt + 1, listener);
                });
            }
        }, listener::onFailure));
    }

    /**
     * Get the model spec for a given language/model type combination. Visible for testing.
     */
    public static ModelSpec getModelSpec(String languageOption, String modelType) {
        return MODEL_RESOLUTION_TABLE.get(languageOption.toUpperCase(Locale.ROOT) + "_" + modelType.toUpperCase(Locale.ROOT));
    }
}
