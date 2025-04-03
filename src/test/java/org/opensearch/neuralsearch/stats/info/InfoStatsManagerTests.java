/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.info;

import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.Version;
import org.opensearch.neuralsearch.settings.NeuralSearchSettingsAccessor;
import org.opensearch.neuralsearch.stats.common.StatSnapshot;
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;
import org.opensearch.neuralsearch.util.PipelineServiceUtil;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

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
}
