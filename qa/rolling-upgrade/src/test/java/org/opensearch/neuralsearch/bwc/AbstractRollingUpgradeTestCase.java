/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.bwc;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import org.junit.Before;
import org.opensearch.common.settings.Settings;
import org.opensearch.neuralsearch.BaseNeuralSearchIT;
import static org.opensearch.neuralsearch.TestUtils.NEURAL_SEARCH_BWC_PREFIX;
import static org.opensearch.neuralsearch.TestUtils.OLD_CLUSTER;
import static org.opensearch.neuralsearch.TestUtils.MIXED_CLUSTER;
import static org.opensearch.neuralsearch.TestUtils.UPGRADED_CLUSTER;
import static org.opensearch.neuralsearch.TestUtils.ROLLING_UPGRADE_FIRST_ROUND;
import static org.opensearch.neuralsearch.TestUtils.BWCSUITE_CLUSTER;
import static org.opensearch.neuralsearch.TestUtils.BWC_VERSION;
import static org.opensearch.neuralsearch.TestUtils.generateModelId;
import org.opensearch.test.rest.OpenSearchRestTestCase;

public abstract class AbstractRollingUpgradeTestCase extends BaseNeuralSearchIT {

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
            .put(OpenSearchRestTestCase.CLIENT_SOCKET_TIMEOUT, "120s")
            .build();
    }

    protected enum ClusterType {
        OLD,
        MIXED,
        UPGRADED;

        public static ClusterType instance(String value) {
            switch (value) {
                case OLD_CLUSTER:
                    return OLD;
                case MIXED_CLUSTER:
                    return MIXED;
                case UPGRADED_CLUSTER:
                    return UPGRADED;
                default:
                    throw new IllegalArgumentException("unknown cluster type: " + value);
            }
        }
    }

    protected final ClusterType getClusterType() {
        return ClusterType.instance(System.getProperty(BWCSUITE_CLUSTER));
    }

    protected final boolean isFirstMixedRound() {
        return Boolean.parseBoolean(System.getProperty(ROLLING_UPGRADE_FIRST_ROUND, "false"));
    }

    protected final Optional<String> getBWCVersion() {
        return Optional.ofNullable(System.getProperty(BWC_VERSION, null));
    }

    protected String uploadTextEmbeddingModel() throws Exception {
        String requestBody = Files.readString(Path.of(classLoader.getResource("processor/UploadModelRequestBody.json").toURI()));
        return registerModelGroupAndGetModelId(requestBody);
    }

    protected String registerModelGroupAndGetModelId(String requestBody) throws Exception {
        String modelGroupRegisterRequestBody = Files.readString(
            Path.of(classLoader.getResource("processor/CreateModelGroupRequestBody.json").toURI())
        );
        String modelGroupId = registerModelGroup(String.format(LOCALE, modelGroupRegisterRequestBody, generateModelId()));
        return uploadModel(String.format(LOCALE, requestBody, modelGroupId));
    }

    protected void createPipelineProcessor(String modelId, String pipelineName) throws Exception {
        String requestBody = Files.readString(Path.of(classLoader.getResource("processor/PipelineConfiguration.json").toURI()));
        createPipelineProcessor(requestBody, pipelineName, modelId);
    }

    protected String uploadTextImageEmbeddingModel() throws Exception {
        String requestBody = Files.readString(Path.of(classLoader.getResource("processor/UploadModelRequestBody.json").toURI()));
        return registerModelGroupAndGetModelId(requestBody);
    }

    protected void createPipelineForTextImageProcessor(String modelId, String pipelineName) throws Exception {
        String requestBody = Files.readString(
            Path.of(classLoader.getResource("processor/PipelineForTextImageProcessorConfiguration.json").toURI())
        );
        createPipelineProcessor(requestBody, pipelineName, modelId);
    }

    protected String uploadSparseEncodingModel() throws Exception {
        String requestBody = Files.readString(
            Path.of(classLoader.getResource("processor/UploadSparseEncodingModelRequestBody.json").toURI())
        );
        return registerModelGroupAndGetModelId(requestBody);
    }

    protected void createPipelineForSparseEncodingProcessor(String modelId, String pipelineName) throws Exception {
        String requestBody = Files.readString(
            Path.of(classLoader.getResource("processor/PipelineForSparseEncodingProcessorConfiguration.json").toURI())
        );
        createPipelineProcessor(requestBody, pipelineName, modelId);
    }

    protected void createPipelineForTextChunkingProcessor(String pipelineName) throws Exception {
        URL pipelineURLPath = classLoader.getResource("processor/PipelineForTextChunkingProcessorConfiguration.json");
        Objects.requireNonNull(pipelineURLPath);
        String requestBody = Files.readString(Path.of(pipelineURLPath.toURI()));
        createPipelineProcessor(requestBody, pipelineName);
    }
}
