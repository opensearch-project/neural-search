/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.bwc.restart;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Optional;

import lombok.SneakyThrows;
import org.junit.After;
import org.junit.Before;
import org.opensearch.common.settings.Settings;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import static org.opensearch.neuralsearch.util.TestUtils.NEURAL_SEARCH_BWC_PREFIX;
import static org.opensearch.neuralsearch.util.TestUtils.CLIENT_TIMEOUT_VALUE;
import static org.opensearch.neuralsearch.util.TestUtils.RESTART_UPGRADE_OLD_CLUSTER;
import static org.opensearch.neuralsearch.util.TestUtils.BWC_VERSION;
import static org.opensearch.neuralsearch.util.TestUtils.generateModelId;
import org.opensearch.test.rest.OpenSearchRestTestCase;

public abstract class AbstractRestartUpgradeRestTestCase extends BaseNeuralSearchIT {

    // Resources to be cleaned up after each test, need to assign the actual values in the test itself
    protected String modelId;
    protected String ingestPipelineName;
    protected String searchPipelineName;
    protected String indexName;

    @Before
    public void initialize() {
        // Initialize variables
        this.modelId = null;
        this.ingestPipelineName = null;
        this.searchPipelineName = null;

        // Creating index name by concatenating "neural-bwc-" prefix with test method name
        // for all the tests in this sub-project
        this.indexName = NEURAL_SEARCH_BWC_PREFIX + getTestName().toLowerCase(Locale.ROOT);
    }

    @SneakyThrows
    @After
    public void cleanUpResources() {
        if (!isRunningAgainstOldCluster()) {
            wipeOfTestResources(this.indexName, this.ingestPipelineName, this.modelId, this.searchPipelineName);
        }
    }

    @Override
    protected final boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Override
    protected final boolean preserveReposUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveTemplatesUponCompletion() {
        return true;
    }

    @Override
    protected final Settings restClientSettings() {
        return Settings.builder()
            .put(super.restClientSettings())
            // increase the timeout here to 90 seconds to handle long waits for a green
            // cluster health. the waits for green need to be longer than a minute to
            // account for delayed shards
            .put(OpenSearchRestTestCase.CLIENT_SOCKET_TIMEOUT, CLIENT_TIMEOUT_VALUE)
            .build();
    }

    @Override
    protected boolean shouldCleanUpResources() {
        // All NEW CLUSTER tests depend on resources created in OLD CLUSTER test cases
        // Before NEW CLUSTER tests run, all OLD CLUSTER test cases will be run first
        // We only want to clean up resources in NEW CLUSTER tests, also we don't want to clean up after each test case finishes
        // this is because the cleanup method will pull every resource and delete, which will impact other tests
        // Overriding the method in base class so that resources won't be accidentally clean up
        return false;
    }

    protected static final boolean isRunningAgainstOldCluster() {
        return Boolean.parseBoolean(System.getProperty(RESTART_UPGRADE_OLD_CLUSTER));
    }

    protected final Optional<String> getBWCVersion() {
        return Optional.ofNullable(System.getProperty(BWC_VERSION, null));
    }

    protected String uploadTextEmbeddingModel() throws Exception {
        String requestBody = Files.readString(Path.of(classLoader.getResource("processor/UploadModelRequestBody.json").toURI()));
        return registerModelGroupAndGetModelId(requestBody);
    }

    protected String registerModelGroupAndGetModelId(final String requestBody) throws Exception {
        String modelGroupRegisterRequestBody = Files.readString(
            Path.of(classLoader.getResource("processor/CreateModelGroupRequestBody.json").toURI())
        );
        String modelGroupId = registerModelGroup(String.format(LOCALE, modelGroupRegisterRequestBody, generateModelId()));
        return uploadModel(String.format(LOCALE, requestBody, modelGroupId));
    }

    protected void createPipelineProcessor(final String modelId, final String pipelineName) throws Exception {
        String requestBody = Files.readString(Path.of(classLoader.getResource("processor/PipelineConfiguration.json").toURI()));
        createPipelineProcessor(requestBody, pipelineName, modelId, null);
    }

    protected String uploadSparseEncodingModel() throws Exception {
        String requestBody = Files.readString(
            Path.of(classLoader.getResource("processor/UploadSparseEncodingModelRequestBody.json").toURI())
        );
        return registerModelGroupAndGetModelId(requestBody);
    }

    protected void createPipelineForTextImageProcessor(final String modelId, final String pipelineName) throws Exception {
        String requestBody = Files.readString(
            Path.of(classLoader.getResource("processor/PipelineForTextImageProcessorConfiguration.json").toURI())
        );
        createPipelineProcessor(requestBody, pipelineName, modelId, null);
    }

    protected void createPipelineForSparseEncodingProcessor(final String modelId, final String pipelineName, final Integer batchSize)
        throws Exception {
        String requestBody = Files.readString(
            Path.of(classLoader.getResource("processor/PipelineForSparseEncodingProcessorConfiguration.json").toURI())
        );
        createPipelineProcessor(requestBody, pipelineName, modelId, batchSize);
    }

    protected void createPipelineForSparseEncodingProcessor(final String modelId, final String pipelineName) throws Exception {
        createPipelineForSparseEncodingProcessor(modelId, pipelineName, null);
    }

    protected void createPipelineForTextChunkingProcessor(String pipelineName) throws Exception {
        String requestBody = Files.readString(
            Path.of(classLoader.getResource("processor/PipelineForTextChunkingProcessorConfiguration.json").toURI())
        );
        createPipelineProcessor(requestBody, pipelineName, "", null);
    }
}
