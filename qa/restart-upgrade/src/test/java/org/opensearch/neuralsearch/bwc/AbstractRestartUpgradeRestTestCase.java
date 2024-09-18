/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.bwc;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Optional;
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

    @Before
    protected String getIndexNameForTest() {
        // Creating index name by concatenating "neural-bwc-" prefix with test method name
        // for all the tests in this sub-project
        return NEURAL_SEARCH_BWC_PREFIX + getTestName().toLowerCase(Locale.ROOT);
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
