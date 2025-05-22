/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.info;

import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.Version;
import org.opensearch.neuralsearch.processor.normalization.L2ScoreNormalizationTechnique;
import org.opensearch.neuralsearch.processor.normalization.MinMaxScoreNormalizationTechnique;
import org.opensearch.neuralsearch.settings.NeuralSearchSettingsAccessor;
import org.opensearch.neuralsearch.stats.common.StatSnapshot;
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;
import org.opensearch.neuralsearch.util.PipelineServiceUtil;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.EnumSet;
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
        when(mockClusterUtil.getClusterMinVersion()).thenReturn(Version.fromString("3.0.0"));
        infoStatsManager = new InfoStatsManager(mockClusterUtil, mockSettingsAccessor, mockPipelineServiceUtil);
    }

    public void test_getStats_returnsAllStats() {
        Map<InfoStatName, StatSnapshot<?>> stats = infoStatsManager.getStats(EnumSet.allOf(InfoStatName.class));
        Set<InfoStatName> allStatNames = EnumSet.allOf(InfoStatName.class);

        assertEquals(allStatNames, stats.keySet());
    }

    public void test_getStats_returnsFilteredStats() {
        Map<InfoStatName, StatSnapshot<?>> stats = infoStatsManager.getStats(EnumSet.of(InfoStatName.CLUSTER_VERSION));

        assertEquals(1, stats.size());
        assertTrue(stats.containsKey(InfoStatName.CLUSTER_VERSION));
        assertEquals("3.0.0", ((SettableInfoStatSnapshot<?>) stats.get(InfoStatName.CLUSTER_VERSION)).getValue());
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
}
