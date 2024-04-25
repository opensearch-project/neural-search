/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import lombok.SneakyThrows;
import org.junit.Before;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.FilterStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.neuralsearch.settings.NeuralSearchSettings;
import org.opensearch.neuralsearch.util.NeuralSearchClusterTestUtils;
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.util.TestUtils.xContentBuilderToMap;

public class NeuralSparseTwoPhaseParametersTests extends OpenSearchTestCase {
    public static int TEST_MAX_WINDOW_SIZE = 100;
    public static float TEST_WINDOW_SIZE_EXPANSION = 6.0f;
    public static float TEST_PRUNING_RATIO = 0.5f;
    public static boolean TEST_ENABLED = false;
    public static NeuralSparseTwoPhaseParameters TWO_PHASE_PARAMETERS = new NeuralSparseTwoPhaseParameters().enabled(TEST_ENABLED)
        .pruning_ratio(TEST_PRUNING_RATIO)
        .window_size_expansion(TEST_WINDOW_SIZE_EXPANSION);
    private ClusterSettings clusterSettings;
    DiscoveryNodes mockDiscoveryNodes = mock(DiscoveryNodes.class);

    @Before
    public void setUpNeuralSparseTwoPhaseParameters() {
        Settings settings = Settings.builder().build();
        final Set<Setting<?>> settingsSet = Stream.concat(
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream(),
            Stream.of(
                NeuralSearchSettings.NEURAL_SPARSE_TWO_PHASE_DEFAULT_ENABLED,
                NeuralSearchSettings.NEURAL_SPARSE_TWO_PHASE_DEFAULT_WINDOW_SIZE_EXPANSION,
                NeuralSearchSettings.NEURAL_SPARSE_TWO_PHASE_DEFAULT_PRUNING_RATIO,
                NeuralSearchSettings.NEURAL_SPARSE_TWO_PHASE_MAX_WINDOW_SIZE
            )
        ).collect(Collectors.toSet());

        clusterSettings = new ClusterSettings(settings, settingsSet);

        ClusterService mockClusterService = mock(ClusterService.class);
        ClusterState mockClusterState = mock(ClusterState.class);

        when(mockClusterService.state()).thenReturn(mockClusterState);
        when(mockClusterState.getNodes()).thenReturn(mockDiscoveryNodes);
        when(mockDiscoveryNodes.getMinNodeVersion()).thenReturn(Version.CURRENT);
        when(mockClusterService.getClusterSettings()).thenReturn(clusterSettings);

        NeuralSearchClusterUtil.instance().initialize(mockClusterService);
        NeuralSparseTwoPhaseParameters.initialize(mockClusterService, settings);
    }

    public void testDefaultValue() {
        NeuralSparseTwoPhaseParameters defaultParameters = NeuralSparseTwoPhaseParameters.getDefaultSettings();
        assertEquals(true, defaultParameters.enabled());
        assertEquals(5f, defaultParameters.window_size_expansion(), 0);
        assertEquals(0.4f, defaultParameters.pruning_ratio(), 0);
        assertEquals(Optional.of(10000), Optional.of(NeuralSparseTwoPhaseParameters.MAX_WINDOW_SIZE));
    }

    public void testUpdateClusterSettingsToUpdateDefaultParameters() {
        Settings newSettings = Settings.builder()
            .put("plugins.neural_search.neural_sparse.two_phase.default_enabled", TEST_ENABLED)
            .put("plugins.neural_search.neural_sparse.two_phase.default_window_size_expansion", TEST_WINDOW_SIZE_EXPANSION)
            .put("plugins.neural_search.neural_sparse.two_phase.default_pruning_ratio", TEST_PRUNING_RATIO)
            .put("plugins.neural_search.neural_sparse.two_phase.max_window_size", TEST_MAX_WINDOW_SIZE)
            .build();

        clusterSettings.applySettings(newSettings);
        NeuralSparseTwoPhaseParameters defaultParameters = NeuralSparseTwoPhaseParameters.getDefaultSettings();
        assertEquals(TEST_ENABLED, defaultParameters.enabled());
        assertEquals(TEST_WINDOW_SIZE_EXPANSION, defaultParameters.window_size_expansion(), 0);
        assertEquals(TEST_PRUNING_RATIO, defaultParameters.pruning_ratio(), 0);
        assertEquals(Optional.of(TEST_MAX_WINDOW_SIZE), Optional.of(NeuralSparseTwoPhaseParameters.MAX_WINDOW_SIZE));
        // restore to default settings
        setUpNeuralSparseTwoPhaseParameters();
    }

    @SneakyThrows
    public void testStreams() {
        NeuralSparseTwoPhaseParameters original = new NeuralSparseTwoPhaseParameters();
        original.enabled(TEST_ENABLED);
        original.window_size_expansion(TEST_WINDOW_SIZE_EXPANSION);
        original.pruning_ratio(TEST_PRUNING_RATIO);

        BytesStreamOutput streamOutput = new BytesStreamOutput();
        original.writeTo(streamOutput);

        FilterStreamInput filterStreamInput = new NamedWriteableAwareStreamInput(
            streamOutput.bytes().streamInput(),
            new NamedWriteableRegistry(
                List.of(new NamedWriteableRegistry.Entry(QueryBuilder.class, MatchAllQueryBuilder.NAME, MatchAllQueryBuilder::new))
            )
        );

        NeuralSparseTwoPhaseParameters copy = new NeuralSparseTwoPhaseParameters(filterStreamInput);
        assertEquals(original, copy);
    }

    @SneakyThrows
    public void testFromXContentWithFullBodyThenSuccess() {
        /*
          {
              "window_size_expansion": 0.5,
              "pruning_ratio": 0.5,
              "enabled": false
          }
        */
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .field(NeuralSparseTwoPhaseParameters.WINDOW_SIZE_EXPANSION.getPreferredName(), TEST_WINDOW_SIZE_EXPANSION)
            .field(NeuralSparseTwoPhaseParameters.PRUNING_RATIO.getPreferredName(), TEST_PRUNING_RATIO)
            .field(NeuralSparseTwoPhaseParameters.ENABLED.getPreferredName(), TEST_ENABLED)
            .endObject();

        XContentParser contentParser = createParser(xContentBuilder);
        contentParser.nextToken();
        NeuralSparseTwoPhaseParameters neuralSparseTwoPhaseParameters = NeuralSparseTwoPhaseParameters.parseFromXContent(contentParser);

        assertEquals(TEST_ENABLED, neuralSparseTwoPhaseParameters.enabled());
        assertEquals(TEST_PRUNING_RATIO, neuralSparseTwoPhaseParameters.pruning_ratio(), 0);
        assertEquals(TEST_WINDOW_SIZE_EXPANSION, neuralSparseTwoPhaseParameters.window_size_expansion(), 0);
    }

    @SneakyThrows
    public void testFromXContentWithFullBodyOntTimeThenDefaultRecoverSuccess() {
        /*
          {
              "window_size_expansion": 0.5,
              "pruning_ratio": 0.5,
              "enabled": false
          }
        */
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .field(NeuralSparseTwoPhaseParameters.WINDOW_SIZE_EXPANSION.getPreferredName(), TEST_WINDOW_SIZE_EXPANSION)
            .field(NeuralSparseTwoPhaseParameters.PRUNING_RATIO.getPreferredName(), TEST_PRUNING_RATIO)
            .field(NeuralSparseTwoPhaseParameters.ENABLED.getPreferredName(), TEST_ENABLED)
            .endObject();

        XContentParser contentParser = createParser(xContentBuilder);
        contentParser.nextToken();
        NeuralSparseTwoPhaseParameters neuralSparseTwoPhaseParameters = NeuralSparseTwoPhaseParameters.parseFromXContent(contentParser);
        assertEquals(TEST_ENABLED, neuralSparseTwoPhaseParameters.enabled());
        assertEquals(TEST_PRUNING_RATIO, neuralSparseTwoPhaseParameters.pruning_ratio(), 0);
        assertEquals(TEST_WINDOW_SIZE_EXPANSION, neuralSparseTwoPhaseParameters.window_size_expansion(), 0);
        neuralSparseTwoPhaseParameters = NeuralSparseTwoPhaseParameters.getDefaultSettings();
        assertEquals(true, neuralSparseTwoPhaseParameters.enabled());
        assertEquals(0.4f, neuralSparseTwoPhaseParameters.pruning_ratio(), 1e-6);
        assertEquals(5.0f, neuralSparseTwoPhaseParameters.window_size_expansion(), 1e-6);

    }

    @SneakyThrows
    public void testFromXContentWithEmptyBodyThenSuccess() {
        /*
          {}
        */
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().endObject();

        XContentParser contentParser = createParser(xContentBuilder);
        contentParser.nextToken();
        NeuralSparseTwoPhaseParameters neuralSparseTwoPhaseParameters = NeuralSparseTwoPhaseParameters.parseFromXContent(contentParser);

        assertEquals(NeuralSparseTwoPhaseParameters.DEFAULT_ENABLED, neuralSparseTwoPhaseParameters.enabled());
        assertEquals(NeuralSparseTwoPhaseParameters.DEFAULT_PRUNING_RATIO, neuralSparseTwoPhaseParameters.pruning_ratio(), 0);
        assertEquals(
            NeuralSparseTwoPhaseParameters.DEFAULT_WINDOW_SIZE_EXPANSION,
            neuralSparseTwoPhaseParameters.window_size_expansion(),
            0
        );
    }

    @SneakyThrows
    public void testFromXContentWithIllegalFieldThenFail() {
        /*
          {"illegal": 1}
        */
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().field("illegal", 1).endObject();

        XContentParser contentParser = createParser(xContentBuilder);
        contentParser.nextToken();

        expectThrows(ParsingException.class, () -> NeuralSparseTwoPhaseParameters.parseFromXContent(contentParser));
    }

    @SneakyThrows
    public void testFromXContentWithIllegalTokenThenFail() {
        /*
          {"enabled":false,"some":{}}
        */
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .field(NeuralSparseTwoPhaseParameters.ENABLED.getPreferredName(), TEST_ENABLED)
            .field("some")
            .startObject()
            .endObject()
            .endObject();

        XContentParser contentParser = createParser(xContentBuilder);
        contentParser.nextToken();

        expectThrows(ParsingException.class, () -> NeuralSparseTwoPhaseParameters.parseFromXContent(contentParser));
    }

    @SneakyThrows
    public void testToXContent() {
        NeuralSparseTwoPhaseParameters neuralSparseTwoPhaseParameters = NeuralSparseTwoPhaseParameters.getDefaultSettings();
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        neuralSparseTwoPhaseParameters.doXContent(builder);
        builder.endObject();
        Map<String, Object> out = xContentBuilderToMap(builder);

        Object outer = out.get(NeuralSparseTwoPhaseParameters.NAME.getPreferredName());
        if (!(outer instanceof Map)) {
            fail();
        }

        Map<String, Object> inner = (Map<String, Object>) outer;

        assertEquals(3, inner.size());

        assertEquals(
            NeuralSparseTwoPhaseParameters.DEFAULT_WINDOW_SIZE_EXPANSION.doubleValue(),
            (Double) inner.get(NeuralSparseTwoPhaseParameters.WINDOW_SIZE_EXPANSION.getPreferredName()),
            1e-6
        );

        assertEquals(
            NeuralSparseTwoPhaseParameters.DEFAULT_PRUNING_RATIO.doubleValue(),
            (Double) inner.get(NeuralSparseTwoPhaseParameters.PRUNING_RATIO.getPreferredName()),
            1e-6
        );

        assertEquals(NeuralSparseTwoPhaseParameters.DEFAULT_ENABLED, inner.get(NeuralSparseTwoPhaseParameters.ENABLED.getPreferredName()));
    }

    @SneakyThrows
    public void testEquals() {
        NeuralSparseTwoPhaseParameters param = NeuralSparseTwoPhaseParameters.getDefaultSettings();
        NeuralSparseTwoPhaseParameters paramSame = NeuralSparseTwoPhaseParameters.getDefaultSettings();
        NeuralSparseTwoPhaseParameters paramDiffRatio = NeuralSparseTwoPhaseParameters.getDefaultSettings()
            .pruning_ratio(TEST_PRUNING_RATIO);
        NeuralSparseTwoPhaseParameters paramDiffWindow = NeuralSparseTwoPhaseParameters.getDefaultSettings()
            .window_size_expansion(TEST_WINDOW_SIZE_EXPANSION);
        NeuralSparseTwoPhaseParameters paramDiffEnabled = NeuralSparseTwoPhaseParameters.getDefaultSettings().enabled(TEST_ENABLED);
        assertEquals(param, paramSame);
        assertNotEquals(null, param);
        assertNotEquals(paramDiffRatio, param);
        assertNotEquals(paramDiffWindow, param);
        assertNotEquals(paramDiffEnabled, param);
    }

    @SneakyThrows
    public void testIsEnabled() {
        NeuralSparseTwoPhaseParameters enabled = new NeuralSparseTwoPhaseParameters().enabled(true);
        NeuralSparseTwoPhaseParameters disabled = new NeuralSparseTwoPhaseParameters().enabled(false);
        assertTrue(NeuralSparseTwoPhaseParameters.isEnabled(enabled));
        assertFalse(NeuralSparseTwoPhaseParameters.isEnabled(disabled));
        assertFalse(NeuralSparseTwoPhaseParameters.isEnabled(null));
    }

    @SneakyThrows
    public void testIsClusterOnOrAfterMinReqVersionForTwoPhaseSearchSupport() {
        ClusterService clusterServiceBefore = NeuralSearchClusterTestUtils.mockClusterService(Version.V_2_13_0);
        NeuralSearchClusterUtil.instance().initialize(clusterServiceBefore);
        assertFalse(NeuralSparseTwoPhaseParameters.isClusterOnOrAfterMinReqVersionForTwoPhaseSearchSupport());
        ClusterService clusterServiceCurrent = NeuralSearchClusterTestUtils.mockClusterService(Version.CURRENT);
        NeuralSearchClusterUtil.instance().initialize(clusterServiceCurrent);
        assertTrue(NeuralSparseTwoPhaseParameters.isClusterOnOrAfterMinReqVersionForTwoPhaseSearchSupport());
    }

    @SneakyThrows
    public void testUpdateSettingsEffectiveness() {
        Settings updatedSettings = Settings.builder()
            .put("plugins.neural_search.neural_sparse.two_phase.default_enabled", true)
            .put("plugins.neural_search.neural_sparse.two_phase.default_window_size_expansion", 10f)
            .put("plugins.neural_search.neural_sparse.two_phase.default_pruning_ratio", 0.8f)
            .build();

        clusterSettings.applySettings(updatedSettings);
        NeuralSparseTwoPhaseParameters updatedParameters = NeuralSparseTwoPhaseParameters.getDefaultSettings();
        assertEquals(true, updatedParameters.enabled());
        assertEquals(10f, updatedParameters.window_size_expansion(), 0);
        assertEquals(0.8f, updatedParameters.pruning_ratio(), 0);

        setUpNeuralSparseTwoPhaseParameters();
    }

}
