/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight.processor;

import java.util.Map;

import lombok.extern.log4j.Log4j2;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.MLModel;
import org.opensearch.neuralsearch.highlight.HighlightConfig;
import org.opensearch.neuralsearch.highlight.HighlightConfigExtractor;
import org.opensearch.neuralsearch.highlight.HighlightContext;
import org.opensearch.neuralsearch.highlight.HighlightContextBuilder;
import org.opensearch.neuralsearch.highlight.HighlightResultApplier;
import org.opensearch.neuralsearch.highlight.HighlightValidator;
import org.opensearch.neuralsearch.highlight.HighlightingStrategy;
import org.opensearch.neuralsearch.highlight.SemanticHighlightingConstants;
import org.opensearch.neuralsearch.highlight.strategies.BatchHighlighter;
import org.opensearch.neuralsearch.highlight.strategies.SingleHighlighter;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.search.pipeline.PipelineProcessingContext;
import org.opensearch.search.pipeline.SearchResponseProcessor;
import org.opensearch.search.pipeline.SystemGeneratedProcessor;

/**
 * System-generated processor that handles semantic highlighting.
 * Automatically applied when semantic highlighting is detected in search queries.
 * This processor extracts all configuration from query-level options rather than
 * storing configuration at pipeline creation time.
 */
@Log4j2
public class SemanticHighlightingProcessor implements SearchResponseProcessor, SystemGeneratedProcessor {

    private final boolean ignoreFailure;
    private final MLCommonsClientAccessor mlClientAccessor;
    private final HighlightConfigExtractor configExtractor;
    private final HighlightValidator validator;
    private final HighlightContextBuilder contextBuilder;
    private final String tag;
    private final String description;

    public SemanticHighlightingProcessor(boolean ignoreFailure, MLCommonsClientAccessor mlClientAccessor) {
        this.ignoreFailure = ignoreFailure;
        this.mlClientAccessor = mlClientAccessor;
        this.configExtractor = new HighlightConfigExtractor();
        this.validator = new HighlightValidator();
        this.contextBuilder = new HighlightContextBuilder();
        this.tag = SemanticHighlightingConstants.DEFAULT_PROCESSOR_TAG;
        this.description = SemanticHighlightingConstants.DEFAULT_PROCESSOR_DESCRIPTION;
    }

    @Override
    public void processResponseAsync(
        SearchRequest request,
        SearchResponse response,
        PipelineProcessingContext responseContext,
        ActionListener<SearchResponse> responseListener
    ) {
        long startTime = System.currentTimeMillis();

        try {
            HighlightConfig config = configExtractor.extract(request, response);

            if (config.getValidationError() != null) {
                log.debug("Configuration extraction failed: {}", config.getValidationError());
                responseListener.onResponse(response);
                return;
            }

            // Check if basic fields are present
            if (!config.hasRequiredFields()) {
                log.debug("Missing required fields for semantic highlighting");
                responseListener.onResponse(response);
                return;
            }

            // Fetch model type and enrich config
            enrichConfigWithModelType(config, response, startTime, responseListener);

        } catch (Exception e) {
            log.error("Error in semantic highlighting processor", e);
            handleError(e, response, responseListener);
        }
    }

    private void enrichConfigWithModelType(
        HighlightConfig config,
        SearchResponse response,
        long startTime,
        ActionListener<SearchResponse> responseListener
    ) {
        mlClientAccessor.getModel(config.getModelId(), ActionListener.wrap(model -> {
            // Add model type to config
            HighlightConfig enrichedConfig = config.withModelType(model.getAlgorithm());

            // Enrich config with batch settings from connector (if available)
            enrichedConfig = enrichConfigFromConnector(enrichedConfig, model);

            // Validate batch inference if enabled
            String batchValidationError = enrichedConfig.validateBatchInference();
            if (batchValidationError != null) {
                enrichedConfig = enrichedConfig.withValidationError(batchValidationError);
            }

            // Additional validation with model type
            enrichedConfig = validator.validate(enrichedConfig, response);

            if (!enrichedConfig.isValid()) {
                responseListener.onFailure(new IllegalArgumentException(enrichedConfig.getValidationError()));
                return;
            }

            // Build context and execute highlighting
            executeHighlighting(enrichedConfig, response, startTime, responseListener);

        }, error -> {
            String errorMsg = "Failed to fetch model information: " + error.getMessage();
            if (ignoreFailure) {
                log.warn(errorMsg);
                responseListener.onResponse(response);
            } else {
                responseListener.onFailure(new RuntimeException(errorMsg, error));
            }
        }));
    }

    private void executeHighlighting(
        HighlightConfig config,
        SearchResponse response,
        long startTime,
        ActionListener<SearchResponse> responseListener
    ) {
        HighlightContext context = contextBuilder.build(config, response, startTime);
        if (context.isEmpty()) {
            log.debug("No valid documents to highlight");
            responseListener.onResponse(response);
            return;
        }

        // Select and create appropriate strategy
        HighlightingStrategy strategy = createStrategy(config);
        log.debug(
            "Using {} for model [{}] of type [{}]",
            config.isBatchInference() ? "BatchHighlighter" : "SingleHighlighter",
            config.getModelId(),
            config.getModelType()
        );

        // Execute highlighting
        strategy.process(context, new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse highlightedResponse) {
                responseListener.onResponse(highlightedResponse);
            }

            @Override
            public void onFailure(Exception e) {
                handleError(e, response, responseListener);
            }
        });
    }

    private HighlightingStrategy createStrategy(HighlightConfig config) {
        HighlightResultApplier applier = new HighlightResultApplier(config.getPreTag(), config.getPostTag());

        if (config.isBatchInference()) {
            return new BatchHighlighter(config.getModelId(), mlClientAccessor, config.getMaxBatchSize(), applier, ignoreFailure);
        }

        return new SingleHighlighter(mlClientAccessor, applier, ignoreFailure);
    }

    private void handleError(Exception e, SearchResponse response, ActionListener<SearchResponse> responseListener) {
        if (ignoreFailure) {
            log.warn("Semantic highlighting failed, returning original response", e);
            responseListener.onResponse(response);
        } else {
            responseListener.onFailure(e);
        }
    }

    /**
     * Enrich the highlight configuration with batch settings from the model's connector.
     * If the model has a connector with batch configuration parameters, use them to
     * configure batch inference settings.
     *
     * @param config The current highlight configuration
     * @param model The ML model containing potential connector information
     * @return The enriched configuration with batch settings from connector
     */
    private HighlightConfig enrichConfigFromConnector(HighlightConfig config, MLModel model) {
        // Only process remote models with connectors
        if (model.getAlgorithm() != FunctionName.REMOTE) {
            return config;
        }

        // Check if model has connector with parameters
        if (model.getConnector() == null || model.getConnector().getParameters() == null) {
            return config;
        }

        Map<String, String> params = model.getConnector().getParameters();

        // Check for batch inference support in connector
        String supportsBatch = params.get(SemanticHighlightingConstants.CONNECTOR_SUPPORTS_BATCH_INFERENCE);
        if (supportsBatch == null) {
            // No batch configuration in connector, use defaults
            return config;
        }

        // Parse batch configuration
        boolean batchInferenceEnabled = Boolean.parseBoolean(supportsBatch);

        if (batchInferenceEnabled) {
            // Get max batch size from connector
            String maxBatchSizeStr = params.get(SemanticHighlightingConstants.CONNECTOR_MAX_BATCH_SIZE);
            int maxBatchSize = SemanticHighlightingConstants.DEFAULT_MAX_INFERENCE_BATCH_SIZE;

            if (maxBatchSizeStr != null) {
                try {
                    maxBatchSize = Integer.parseInt(maxBatchSizeStr);
                    // Validate batch size is positive
                    if (maxBatchSize <= 0) {
                        maxBatchSize = SemanticHighlightingConstants.DEFAULT_MAX_INFERENCE_BATCH_SIZE;
                    }
                    // No upper limit - trust the connector configuration
                } catch (NumberFormatException e) {
                    log.warn("Invalid max_batch_size in connector: {}, using default", maxBatchSizeStr);
                }
            }

            log.debug("Configuring batch inference from connector: enabled={}, maxBatchSize={}", true, maxBatchSize);
            return config.toBuilder().batchInference(true).maxBatchSize(maxBatchSize).build();
        } else {
            // Connector explicitly disables batch inference
            log.debug("Connector disables batch inference");
            return config.toBuilder().batchInference(false).maxBatchSize(1).build();
        }
    }

    @Override
    public SearchResponse processResponse(SearchRequest request, SearchResponse response) {
        throw new UnsupportedOperationException("Semantic highlighting processor requires async processing");
    }

    @Override
    public String getType() {
        return SemanticHighlightingConstants.PROCESSOR_TYPE;
    }

    @Override
    public String getTag() {
        return tag;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public boolean isIgnoreFailure() {
        return ignoreFailure;
    }

    @Override
    public SystemGeneratedProcessor.ExecutionStage getExecutionStage() {
        // Execute after user-defined processors to allow them to modify the response first
        return ExecutionStage.POST_USER_DEFINED;
    }
}
