/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.apache.hc.core5.http.ParseException;
import org.opensearch.ml.common.model.MLModelState;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.neuralsearch.util.TestUtils.INGEST_PIPELINE_TYPE;
import static org.opensearch.neuralsearch.util.TestUtils.SEARCH_PIPELINE_TYPE;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public abstract class BaseUpgradeTestCase extends BaseNeuralSearchIT {
    // Shared model IDs for reuse across tests - used by rolling-upgrade and restart-upgrade tests
    private static String sharedTextEmbeddingModelId = null;
    private static String sharedTextImageEmbeddingModelId = null;
    private static String sharedSparseEmbeddingModelId = null;
    private static final Set<String> DEPLOYED_MODEL_IDS = new HashSet<>();

    @Override
    protected boolean shouldCleanUpResources() {
        // All UPGRADE tests depend on resources created in OLD and MIXED test cases
        // Before UPGRADE tests run, all OLD and MIXED test cases will be run first
        // We only want to clean up resources in upgrade tests, also we don't want to clean up after each test case finishes
        // this is because the cleanup method will pull every resource and delete, which will impact other tests
        // Overriding the method in base class so that resources won't be accidentally clean up
        return false;
    }

    @Override
    public boolean preserveClusterUponCompletion() {
        // Otherwise, the cluster setting to enable ml-common is reset and the model is undeployed
        return true;
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveReposUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveTemplatesUponCompletion() {
        return true;
    }

    /**
     * Check if a model is being used by any ingest or search pipeline
     * @param modelId the model ID to check
     * @return true if the model is used by any pipeline, false otherwise
     */
    @SneakyThrows
    protected boolean isModelUsedByAnyPipeline(final String modelId) {
        Map<String, Object> ingestPipelines = retrievePipelines(INGEST_PIPELINE_TYPE, null);
        Map<String, Object> searchPipelines = retrievePipelines(SEARCH_PIPELINE_TYPE, null);

        return isPipelineMapContainingModel(ingestPipelines, modelId) || isPipelineMapContainingModel(searchPipelines, modelId);
    }

    /**
     * Check if a model is being used by any index
     * @param modelId the model ID to check
     * @return true if the model is used by any pipeline, false otherwise
     */
    @SneakyThrows
    protected boolean isModelUsedByAnyIndex(final String modelId) {
        List<Map<String, Object>> indexMappings = retrieveIndices();
        for (Map<String, Object> index : indexMappings) {
            if (isIndexContainingModel(index, modelId)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Check if any pipeline in the given pipeline map contains the specified model
     * @param pipelinesMap the map of pipelines to check
     * @param modelId the model ID to search for
     * @return true if any pipeline contains the model, false otherwise
     */
    @SneakyThrows
    protected boolean isPipelineMapContainingModel(final Map<String, Object> pipelinesMap, final String modelId) {
        if (pipelinesMap == null || pipelinesMap.isEmpty()) {
            return false;
        }
        return pipelinesMap.toString().contains(modelId);
    }

    /**
     * Check if given index contains the specified model
     * @param indexMap the map of pipelines to check
     * @param modelId the model ID to search for
     * @return true if index contains the model, false otherwise
     */
    @SneakyThrows
    protected boolean isIndexContainingModel(final Map<String, Object> indexMap, final String modelId) {
        if (indexMap == null || indexMap.isEmpty()) {
            return false;
        }
        return indexMap.toString().contains(modelId);
    }

    @SneakyThrows
    @Override
    protected void wipeOfTestResources(
        final String indexName,
        final String ingestPipeline,
        final String modelId,
        final String searchPipeline
    ) {
        synchronized (BaseUpgradeTestCase.class) {
            if (ingestPipeline != null) {
                deleteIngestPipeline(ingestPipeline);
            }
            if (searchPipeline != null) {
                deleteSearchPipeline(searchPipeline);
            }
            if (modelId != null) {
                if (!isModelUsedByAnyPipeline(modelId) && !isModelUsedByAnyIndex(modelId)) {
                    deleteModel(modelId);
                    DEPLOYED_MODEL_IDS.remove(modelId);
                    if (modelId.equals(sharedTextEmbeddingModelId)) {
                        sharedTextEmbeddingModelId = null;
                    }
                    if (modelId.equals(sharedTextImageEmbeddingModelId)) {
                        sharedTextImageEmbeddingModelId = null;
                    }
                    if (modelId.equals(sharedSparseEmbeddingModelId)) {
                        sharedSparseEmbeddingModelId = null;
                    }
                }
            }
            if (indexName != null) {
                deleteIndex(indexName);
            }
        }
    }

    protected boolean isModelReadyForInference(@NonNull final String modelId) throws IOException, ParseException {
        MLModelState state = getModelState(modelId);
        return MLModelState.LOADED.equals(state) || MLModelState.DEPLOYED.equals(state);
    }

    protected void waitForModelToBeReady(String modelId) throws Exception {
        int maxAttempts = 30;
        int waitTimeInSeconds = 2;

        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            if (isModelReadyForInference(modelId)) {
                logger.info("Model {} is ready for inference after {} attempts", modelId, attempt + 1);
                return;
            }
            logger.info("Waiting for model {} to be ready. Attempt {}/{}", modelId, attempt + 1, maxAttempts);
            Thread.sleep(waitTimeInSeconds * 1000);
        }
        throw new RuntimeException("Model " + modelId + " failed to be ready for inference after " + maxAttempts + " attempts");
    }

    // ============== SHARED MODEL MANAGEMENT METHODS ==============
    // These methods provide shared model functionality for upgrade tests

    static private synchronized String getOrCreateSharedTextEmbeddingModel(BaseUpgradeTestCase baseNeuralSearchIT) throws Exception {
        if (sharedTextEmbeddingModelId == null) {
            String requestBody = Files.readString(
                Path.of(baseNeuralSearchIT.classLoader.getResource("processor/UploadModelRequestBody.json").toURI())
            );
            sharedTextEmbeddingModelId = baseNeuralSearchIT.registerModelGroupAndUploadModel(requestBody);
            baseNeuralSearchIT.doLoadAndWaitForModelToBeReady(sharedTextEmbeddingModelId);
        }
        return sharedTextEmbeddingModelId;
    }

    static private synchronized String getOrCreateSharedTextImageEmbeddingModel(BaseUpgradeTestCase baseNeuralSearchIT) throws Exception {
        if (sharedTextImageEmbeddingModelId == null) {
            String requestBody = Files.readString(
                Path.of(baseNeuralSearchIT.classLoader.getResource("processor/UploadModelRequestBody.json").toURI())
            );
            sharedTextImageEmbeddingModelId = baseNeuralSearchIT.registerModelGroupAndUploadModel(requestBody);
            baseNeuralSearchIT.doLoadAndWaitForModelToBeReady(sharedTextImageEmbeddingModelId);
        }
        return sharedTextImageEmbeddingModelId;
    }

    static private synchronized String getOrCreateSharedSparseEmbeddingModel(BaseUpgradeTestCase baseNeuralSearchIT) throws Exception {
        if (sharedSparseEmbeddingModelId == null) {
            String requestBody = Files.readString(
                Path.of(baseNeuralSearchIT.classLoader.getResource("processor/UploadSparseEncodingModelRequestBody.json").toURI())
            );
            sharedSparseEmbeddingModelId = baseNeuralSearchIT.registerModelGroupAndUploadModel(requestBody);
            baseNeuralSearchIT.doLoadAndWaitForModelToBeReady(sharedSparseEmbeddingModelId);
        }
        return sharedSparseEmbeddingModelId;
    }

    private void doLoadAndWaitForModelToBeReady(String modelId) throws Exception {
        MLModelState state = getModelState(modelId);
        logger.info("Model state: " + state);
        if (MLModelState.LOADED.equals(state) || MLModelState.DEPLOYED.equals(state)) {
            logger.info("Model is already deployed. Skip loading.");
            return;
        }
        loadModel(modelId);
        waitForModelToBeReady(modelId);
    }

    static private synchronized void loadAndWaitForModelToBeReady(BaseUpgradeTestCase baseNeuralSearchIT, String modelId) throws Exception {
        if (DEPLOYED_MODEL_IDS.contains(modelId)) {
            return;
        }
        // To avoid race condition with auto deploy after node replacement
        Thread.sleep(10000);
        baseNeuralSearchIT.doLoadAndWaitForModelToBeReady(modelId);
        DEPLOYED_MODEL_IDS.add(modelId);
    }

    protected void loadAndWaitForModelToBeReady(String modelId) throws Exception {
        loadAndWaitForModelToBeReady(this, modelId);
    }

    protected String uploadTextEmbeddingModel() throws Exception {
        return getOrCreateSharedTextEmbeddingModel(this);
    }

    protected String uploadTextImageEmbeddingModel() throws Exception {
        return getOrCreateSharedTextImageEmbeddingModel(this);
    }

    protected String uploadSparseEncodingModel() throws Exception {
        return getOrCreateSharedSparseEmbeddingModel(this);
    }
}
