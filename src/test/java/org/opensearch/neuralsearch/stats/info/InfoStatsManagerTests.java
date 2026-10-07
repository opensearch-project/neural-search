/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.info;

import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.Version;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.neuralsearch.settings.NeuralSearchSettings;
import org.opensearch.neuralsearch.processor.normalization.L2ScoreNormalizationTechnique;
import org.opensearch.neuralsearch.processor.normalization.MinMaxScoreNormalizationTechnique;
import org.opensearch.neuralsearch.settings.NeuralSearchSettingsAccessor;
import org.opensearch.neuralsearch.stats.common.StatSnapshot;
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;
import org.opensearch.neuralsearch.util.PipelineServiceUtil;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.mockito.Mockito.when;

public class InfoStatsManagerTests extends OpenSearchTestCase {
    @Mock
    private NeuralSearchSettingsAccessor mockSettingsAccessor;
    @Mock
    private NeuralSearchClusterUtil mockClusterUtil;
    @Mock
    private PipelineServiceUtil mockPipelineServiceUtil;

    private InfoStatsManager infoStatsManager;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        when(mockPipelineServiceUtil.getIngestPipelineConfigs()).thenReturn(new ArrayList<>());
        when(mockPipelineServiceUtil.getSearchPipelineConfigs()).thenReturn(new ArrayList<>());
        when(mockClusterUtil.getClusterMinVersion()).thenReturn(Version.CURRENT);
        when(mockClusterUtil.getAllIndexMappings()).thenReturn(new ArrayList<>());
        infoStatsManager = new InfoStatsManager(mockClusterUtil, mockSettingsAccessor, mockPipelineServiceUtil);
    }

    public void test_getStats_countsSparseVectorFieldsPerIndex() {
        when(mockClusterUtil.getAllIndexMappings()).thenReturn(
            List.of(
                // Two native engine fields, one of them nested under an object field
                mappingOf(
                    Map.of(
                        "native_field",
                        sparseVectorField("native"),
                        "parent",
                        Map.of("properties", Map.of("nested_native_field", sparseVectorField("native")))
                    )
                ),
                // An explicit lucene engine field and one that defaults to lucene
                mappingOf(Map.of("lucene_field", sparseVectorField("lucene"), "default_field", sparseVectorField(null))),
                // No sparse vector field at all
                mappingOf(Map.of("text_field", Map.of("type", "text")))
            )
        );

        Map<InfoStatName, StatSnapshot<?>> stats = infoStatsManager.getStats(EnumSet.allOf(InfoStatName.class));

        assertEquals(2L, getCountable(stats, InfoStatName.SPARSE_VECTOR_INDICES));
        assertEquals(4L, getCountable(stats, InfoStatName.SPARSE_VECTOR_FIELDS));
        assertEquals(1L, getCountable(stats, InfoStatName.SPARSE_NATIVE_ENGINE_INDICES));
        assertEquals(2L, getCountable(stats, InfoStatName.SPARSE_NATIVE_ENGINE_FIELDS));
    }

    public void test_getStats_returnsZeroSparseFieldStats_whenNoIndexHasTheField() {
        when(mockClusterUtil.getAllIndexMappings()).thenReturn(List.of(mappingOf(Map.of("text_field", Map.of("type", "text")))));

        Map<InfoStatName, StatSnapshot<?>> stats = infoStatsManager.getStats(EnumSet.allOf(InfoStatName.class));

        assertEquals(0L, getCountable(stats, InfoStatName.SPARSE_VECTOR_INDICES));
        assertEquals(0L, getCountable(stats, InfoStatName.SPARSE_VECTOR_FIELDS));
        assertEquals(0L, getCountable(stats, InfoStatName.SPARSE_NATIVE_ENGINE_INDICES));
        assertEquals(0L, getCountable(stats, InfoStatName.SPARSE_NATIVE_ENGINE_FIELDS));
    }

    public void test_getStats_ignoresMappingWithoutProperties() {
        when(mockClusterUtil.getAllIndexMappings()).thenReturn(List.of(Collections.emptyMap()));

        Map<InfoStatName, StatSnapshot<?>> stats = infoStatsManager.getStats(EnumSet.allOf(InfoStatName.class));

        assertEquals(0L, getCountable(stats, InfoStatName.SPARSE_VECTOR_FIELDS));
    }

    public void test_getStats_ignoresMalformedMappingEntries() {
        // A properties value that is not a map, and a sparse vector field with no method, are both
        // shapes the mapper rejects, so they only reach here if cluster state holds something odd
        when(mockClusterUtil.getAllIndexMappings()).thenReturn(
            List.of(mappingOf(Map.of("not_a_field", "text", "no_method_field", Map.of("type", "sparse_vector"))))
        );

        Map<InfoStatName, StatSnapshot<?>> stats = infoStatsManager.getStats(EnumSet.allOf(InfoStatName.class));

        assertEquals(1L, getCountable(stats, InfoStatName.SPARSE_VECTOR_INDICES));
        assertEquals(1L, getCountable(stats, InfoStatName.SPARSE_VECTOR_FIELDS));
        assertEquals(0L, getCountable(stats, InfoStatName.SPARSE_NATIVE_ENGINE_INDICES));
        assertEquals(0L, getCountable(stats, InfoStatName.SPARSE_NATIVE_ENGINE_FIELDS));
    }

    public void test_getStats_whenIndexMappingsUnavailable_thenNoSparseFieldStats() {
        when(mockClusterUtil.getAllIndexMappings()).thenReturn(null);

        Map<InfoStatName, StatSnapshot<?>> stats = infoStatsManager.getStats(EnumSet.allOf(InfoStatName.class));

        assertEquals(0L, getCountable(stats, InfoStatName.SPARSE_VECTOR_INDICES));
        assertEquals(0L, getCountable(stats, InfoStatName.SPARSE_VECTOR_FIELDS));
    }

    public void test_getStats_returnsAllStats() {
        Map<InfoStatName, StatSnapshot<?>> stats = infoStatsManager.getStats(EnumSet.allOf(InfoStatName.class));
        Set<InfoStatName> allStatNames = EnumSet.allOf(InfoStatName.class);

        assertEquals(allStatNames, stats.keySet());
    }

    public void test_getStats_returnsAllStats_emptyPipelineConfigs() {
        when(mockPipelineServiceUtil.getIngestPipelineConfigs()).thenReturn(List.of(Collections.emptyMap()));
        when(mockPipelineServiceUtil.getSearchPipelineConfigs()).thenReturn(List.of(Collections.emptyMap()));

        Map<InfoStatName, StatSnapshot<?>> stats = infoStatsManager.getStats(EnumSet.allOf(InfoStatName.class));
        Set<InfoStatName> allStatNames = EnumSet.allOf(InfoStatName.class);

        assertEquals(allStatNames, stats.keySet());
    }

    public void test_getStats_returnsAllStats_partiallyEmptyPipelineConfigs() {
        when(mockPipelineServiceUtil.getIngestPipelineConfigs()).thenReturn(List.of(Collections.emptyMap()));
        when(mockPipelineServiceUtil.getSearchPipelineConfigs()).thenReturn(List.of(Map.of(
                InfoStatsManager.PHASE_PROCESSORS_KEY, List.of(Collections.emptyMap())
        )));

        Map<InfoStatName, StatSnapshot<?>> stats = infoStatsManager.getStats(EnumSet.allOf(InfoStatName.class));
        Set<InfoStatName> allStatNames = EnumSet.allOf(InfoStatName.class);

        assertEquals(allStatNames, stats.keySet());
    }

    public void test_getStats_returnsFilteredStats() {
        Map<InfoStatName, StatSnapshot<?>> stats = infoStatsManager.getStats(EnumSet.of(InfoStatName.CLUSTER_VERSION));

        assertEquals(1, stats.size());
        assertTrue(stats.containsKey(InfoStatName.CLUSTER_VERSION));
        assertEquals(Version.CURRENT.toString(), ((SettableInfoStatSnapshot<?>) stats.get(InfoStatName.CLUSTER_VERSION)).getValue());
    }

    public void test_callNullableIncrementer() {
        // Create stats map with two techniques
        Map<InfoStatName, CountableInfoStatSnapshot> stats = Map.of(
            InfoStatName.NORM_TECHNIQUE_L2_PROCESSORS,
            new CountableInfoStatSnapshot(InfoStatName.NORM_TECHNIQUE_L2_PROCESSORS),
            InfoStatName.NORM_TECHNIQUE_MINMAX_PROCESSORS,
            new CountableInfoStatSnapshot(InfoStatName.NORM_TECHNIQUE_MINMAX_PROCESSORS)
        );

        // Create incrementer map with only 1 technique
        Map<String, Consumer<Map<InfoStatName, CountableInfoStatSnapshot>>> incrementerMap = Map.of(
            L2ScoreNormalizationTechnique.TECHNIQUE_NAME,
            statsParam -> infoStatsManager.increment(statsParam, InfoStatName.NORM_TECHNIQUE_L2_PROCESSORS)
        );

        // Calling nullable incrementer should only increment the technique in the map
        infoStatsManager.callNullableIncrementer(stats, incrementerMap.get(L2ScoreNormalizationTechnique.TECHNIQUE_NAME));
        infoStatsManager.callNullableIncrementer(stats, incrementerMap.get(MinMaxScoreNormalizationTechnique.TECHNIQUE_NAME));

        assertEquals(1, (long) stats.get(InfoStatName.NORM_TECHNIQUE_L2_PROCESSORS).getValue());
        assertEquals(0, (long) stats.get(InfoStatName.NORM_TECHNIQUE_MINMAX_PROCESSORS).getValue());
    }

    /**
     * The resolver-enabled info stat is read off the cluster settings on every call rather than cached, so that an
     * operator turning fused mode on or off is reflected by the next stats read. These four cases are the whole decision:
     * the setting in force, either way; and the two ways it cannot be read, which must fall back to the setting's own
     * default instead of failing a stats call for the sake of an adoption metric.
     */
    public void test_hybridFusionEnabled_readsTheSettingInForce() {
        withClusterSettings(Settings.builder().put(NeuralSearchSettings.HYBRID_FUSION_ENABLED.getKey(), true).build());

        assertEquals(Boolean.TRUE, fusionEnabledStat());
    }

    public void test_hybridFusionEnabled_reportsFalseWhenTheSettingIsOff() {
        withClusterSettings(Settings.builder().put(NeuralSearchSettings.HYBRID_FUSION_ENABLED.getKey(), false).build());

        assertEquals("explicitly off is not the same fact as unreadable, but both read false here", Boolean.FALSE, fusionEnabledStat());
    }

    public void test_hybridFusionEnabled_fallsBackToTheDefaultWhenTheSettingIsNotRegistered() {
        ClusterService clusterService = org.mockito.Mockito.mock(ClusterService.class);
        // A ClusterSettings that does not know this setting: get(...) throws SettingsException, which must not escape.
        when(clusterService.getClusterSettings()).thenReturn(new ClusterSettings(Settings.EMPTY, Set.<Setting<?>>of()));
        when(mockClusterUtil.getClusterService()).thenReturn(clusterService);

        assertEquals(
            "an unreadable setting falls back rather than failing the stats read",
            NeuralSearchSettings.HYBRID_FUSION_ENABLED.getDefault(Settings.EMPTY),
            fusionEnabledStat()
        );
    }

    public void test_hybridFusionEnabled_fallsBackWhenThereAreNoClusterSettingsToRead() {
        ClusterService clusterService = org.mockito.Mockito.mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(null);
        when(mockClusterUtil.getClusterService()).thenReturn(clusterService);

        assertEquals(
            "a node still starting has a cluster service but nothing to read from it",
            NeuralSearchSettings.HYBRID_FUSION_ENABLED.getDefault(Settings.EMPTY),
            fusionEnabledStat()
        );
    }

    /** Puts a real {@link ClusterSettings} with this setting registered behind the mocked cluster service. */
    private void withClusterSettings(final Settings settings) {
        ClusterService clusterService = org.mockito.Mockito.mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(settings, Set.<Setting<?>>of(NeuralSearchSettings.HYBRID_FUSION_ENABLED))
        );
        when(mockClusterUtil.getClusterService()).thenReturn(clusterService);
    }

    private Object fusionEnabledStat() {
        return infoStatsManager.getStats(EnumSet.of(InfoStatName.HYBRID_FUSION_ENABLED)).get(InfoStatName.HYBRID_FUSION_ENABLED).getValue();
    }

    private Map<String, Object> mappingOf(Map<String, Object> properties) {
        return Map.of("properties", properties);
    }

    private Map<String, Object> sparseVectorField(String engine) {
        Map<String, Object> method = new HashMap<>();
        method.put("name", "seismic");
        if (engine != null) {
            method.put("engine", engine);
        }
        return Map.of("type", "sparse_vector", "method", method);
    }

    private long getCountable(Map<InfoStatName, StatSnapshot<?>> stats, InfoStatName statName) {
        return ((CountableInfoStatSnapshot) stats.get(statName)).getValue();
    }
}
