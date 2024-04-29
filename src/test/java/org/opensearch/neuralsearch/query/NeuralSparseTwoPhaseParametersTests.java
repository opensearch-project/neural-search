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
import org.opensearch.neuralsearch.util.NeuralSearchClusterTestUtils;
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.neuralsearch.util.TestUtils.xContentBuilderToMap;

public class NeuralSparseTwoPhaseParametersTests extends OpenSearchTestCase {
    public static float TEST_WINDOW_SIZE_EXPANSION = 6.0f;
    public static float TEST_PRUNING_RATIO = 0.5f;
    public static boolean TEST_ENABLED = false;
    public static NeuralSparseTwoPhaseParameters TWO_PHASE_PARAMETERS = new NeuralSparseTwoPhaseParameters().enabled(TEST_ENABLED)
        .pruning_ratio(TEST_PRUNING_RATIO)
        .window_size_expansion(TEST_WINDOW_SIZE_EXPANSION);
    private static final String TWO_PHASE_ENABLED_SETTING_KEY = "index.neural_sparse.two_phase.default_enabled";
    private static final String TWO_PHASE_WINDOW_SIZE_EXPANSION_SETTING_KEY = "index.neural_sparse.two_phase.default_window_size_expansion";
    private static final String TWO_PHASE_PRUNE_RATIO_SETTING_KEY = "index.neural_sparse.two_phase.default_pruning_ratio";
    private ClusterSettings clusterSettings;
    DiscoveryNodes mockDiscoveryNodes = mock(DiscoveryNodes.class);

    @Before
    public void setUpNeuralSparseTwoPhaseParameters() {
        Settings settings = Settings.builder().build();
        final Set<Setting<?>> settingsSet = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        clusterSettings = new ClusterSettings(settings, settingsSet);
        ClusterService mockClusterService = mock(ClusterService.class);
        ClusterState mockClusterState = mock(ClusterState.class);
        when(mockClusterService.state()).thenReturn(mockClusterState);
        when(mockClusterState.getNodes()).thenReturn(mockDiscoveryNodes);
        when(mockDiscoveryNodes.getMinNodeVersion()).thenReturn(Version.CURRENT);
        when(mockDiscoveryNodes.getMaxNodeVersion()).thenReturn(Version.CURRENT);
        when(mockClusterService.getClusterSettings()).thenReturn(clusterSettings);
        NeuralSearchClusterUtil.instance().initialize(mockClusterService);
    }

    public void testDefaultValue() {
        NeuralSparseTwoPhaseParameters defaultParameters = new NeuralSparseTwoPhaseParameters();
        assertNull(defaultParameters.enabled());
        assertNull(defaultParameters.window_size_expansion());
        assertNull(defaultParameters.pruning_ratio());
    }

    public void testUpdateClusterSettingsToUpdateDefaultParameters() {
        Settings newSettings = Settings.builder()
            .put(TWO_PHASE_ENABLED_SETTING_KEY, TEST_ENABLED)
            .put(TWO_PHASE_WINDOW_SIZE_EXPANSION_SETTING_KEY, TEST_WINDOW_SIZE_EXPANSION)
            .put(TWO_PHASE_PRUNE_RATIO_SETTING_KEY, TEST_PRUNING_RATIO)
            .build();
        NeuralSparseTwoPhaseParameters defaultParameters = new NeuralSparseTwoPhaseParameters(newSettings);
        assertEquals(TEST_ENABLED, defaultParameters.enabled());
        assertEquals(TEST_WINDOW_SIZE_EXPANSION, defaultParameters.window_size_expansion(), 0);
        assertEquals(TEST_PRUNING_RATIO, defaultParameters.pruning_ratio(), 0);
        // restore to default settings
        setUpNeuralSparseTwoPhaseParameters();
    }

    private Settings getDefaultTwoPhaseParameterSettings() {
        return Settings.builder()
            .put(TWO_PHASE_ENABLED_SETTING_KEY, true)
            .put(TWO_PHASE_WINDOW_SIZE_EXPANSION_SETTING_KEY, 5.0f)
            .put(TWO_PHASE_PRUNE_RATIO_SETTING_KEY, 0.4f)
            .build();
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
    }

    @SneakyThrows
    public void testFromXContentWithEmptyBodyThenSuccess() {
        /*
          {}
        */
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().endObject();
        XContentParser contentParser = createParser(xContentBuilder);
        contentParser.nextToken();
        NeuralSparseTwoPhaseParameters.parseFromXContent(contentParser);
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
        NeuralSparseTwoPhaseParameters neuralSparseTwoPhaseParameters = new NeuralSparseTwoPhaseParameters(
            getDefaultTwoPhaseParameterSettings()
        );
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

        assertEquals(5.0f, (Double) inner.get(NeuralSparseTwoPhaseParameters.WINDOW_SIZE_EXPANSION.getPreferredName()), 1e-6);

        assertEquals(0.4f, (Double) inner.get(NeuralSparseTwoPhaseParameters.PRUNING_RATIO.getPreferredName()), 1e-6);

        assertEquals(true, inner.get(NeuralSparseTwoPhaseParameters.ENABLED.getPreferredName()));
    }

    @SneakyThrows
    public void testEquals() {
        NeuralSparseTwoPhaseParameters param = new NeuralSparseTwoPhaseParameters(getDefaultTwoPhaseParameterSettings());
        NeuralSparseTwoPhaseParameters paramSame = new NeuralSparseTwoPhaseParameters(getDefaultTwoPhaseParameterSettings());
        NeuralSparseTwoPhaseParameters paramDiffRatio = new NeuralSparseTwoPhaseParameters(getDefaultTwoPhaseParameterSettings())
            .pruning_ratio(TEST_PRUNING_RATIO);
        NeuralSparseTwoPhaseParameters paramDiffWindow = new NeuralSparseTwoPhaseParameters(getDefaultTwoPhaseParameterSettings())
            .window_size_expansion(TEST_WINDOW_SIZE_EXPANSION);
        NeuralSparseTwoPhaseParameters paramDiffEnabled = new NeuralSparseTwoPhaseParameters(getDefaultTwoPhaseParameterSettings()).enabled(
            TEST_ENABLED
        );
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
        ClusterService clusterServiceBefore = NeuralSearchClusterTestUtils.mockClusterService(Version.V_2_13_0, Version.CURRENT);
        NeuralSearchClusterUtil.instance().initialize(clusterServiceBefore);
        assertFalse(NeuralSparseTwoPhaseParameters.isClusterOnSameVersionForTwoPhaseSearchSupport());
        ClusterService clusterServiceCurrent = NeuralSearchClusterTestUtils.mockClusterService(Version.CURRENT, Version.CURRENT);
        NeuralSearchClusterUtil.instance().initialize(clusterServiceCurrent);
        assertTrue(NeuralSparseTwoPhaseParameters.isClusterOnSameVersionForTwoPhaseSearchSupport());
    }

}
