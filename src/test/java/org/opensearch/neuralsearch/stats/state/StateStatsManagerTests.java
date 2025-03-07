/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.state;

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

public class StateStatsManagerTests extends OpenSearchTestCase {
    @Mock
    private NeuralSearchSettingsAccessor mockSettingsAccessor;
    @Mock
    private NeuralSearchClusterUtil mockClusterUtil;
    @Mock
    private PipelineServiceUtil mockPipelineServiceUtil;

    private StateStatsManager stateStatsManager;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        when(mockPipelineServiceUtil.getIngestPipelineConfigs()).thenReturn(new ArrayList<>());
        when(mockClusterUtil.getClusterMinVersion()).thenReturn(Version.fromString("3.0.0"));
        stateStatsManager = new StateStatsManager(mockSettingsAccessor, mockClusterUtil, mockPipelineServiceUtil);
    }

    public void test_getStats_returnsAllStats() {
        Map<StateStatName, StatSnapshot<?>> stats = stateStatsManager.getStats(EnumSet.allOf(StateStatName.class));
        Set<StateStatName> allStatNames = EnumSet.allOf(StateStatName.class);

        assertEquals(allStatNames, stats.keySet());
    }

    public void test_getStats_returnsFilteredStats() {
        Map<StateStatName, StatSnapshot<?>> stats = stateStatsManager.getStats(EnumSet.of(StateStatName.CLUSTER_VERSION));

        assertEquals(1, stats.size());
        assertTrue(stats.containsKey(StateStatName.CLUSTER_VERSION));
        assertEquals("3.0.0", ((SettableStateStatSnapshot<?>) stats.get(StateStatName.CLUSTER_VERSION)).getValue());
    }
}
