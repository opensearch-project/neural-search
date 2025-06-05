/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.common;

import org.opensearch.Version;
import org.opensearch.neuralsearch.stats.events.EventStatName;
import org.opensearch.neuralsearch.stats.info.InfoStatName;
import org.opensearch.test.OpenSearchTestCase;

import java.util.EnumSet;

public class MinClusterVersionUtilTests extends OpenSearchTestCase {
    public void testStatsByVersion_currentVersionHasAllStats() {
        // Current version should always contain all stats
        EnumSet<InfoStatName> infoStats = MinClusterVersionUtil.getInfoStatsAvailableInVersion(Version.CURRENT);
        assertEquals(infoStats, EnumSet.allOf(InfoStatName.class));

        EnumSet<EventStatName> eventStats = MinClusterVersionUtil.getEventStatsAvailableInVersion(Version.CURRENT);
        assertEquals(eventStats, EnumSet.allOf(EventStatName.class));
    }

    public void testStatsByVersion_versionContainsCorrectStats() {
        // 3.0 contains base stats
        EnumSet<InfoStatName> infoStats = MinClusterVersionUtil.getInfoStatsAvailableInVersion(Version.V_3_0_0);
        assertEquals(infoStats, MinClusterVersionUtil.infoStatsByVersion.get(Version.V_3_0_0));

        EnumSet<EventStatName> eventStats = MinClusterVersionUtil.getEventStatsAvailableInVersion(Version.V_3_0_0);
        assertEquals(eventStats, MinClusterVersionUtil.eventStatsByVersion.get(Version.V_3_0_0));

        // 3.1 should cumulative stats should contain stats from previous version and current version
        infoStats = MinClusterVersionUtil.getInfoStatsAvailableInVersion(Version.V_3_1_0);
        EnumSet<InfoStatName> cumulativeInfoStats = EnumSet.noneOf(InfoStatName.class);
        cumulativeInfoStats.addAll(MinClusterVersionUtil.infoStatsByVersion.get(Version.V_3_0_0));
        cumulativeInfoStats.addAll(MinClusterVersionUtil.infoStatsByVersion.get(Version.V_3_1_0));
        assertEquals(infoStats, cumulativeInfoStats);

        eventStats = MinClusterVersionUtil.getEventStatsAvailableInVersion(Version.V_3_1_0);
        EnumSet<EventStatName> cumulativeEventStats = EnumSet.noneOf(EventStatName.class);
        cumulativeEventStats.addAll(MinClusterVersionUtil.eventStatsByVersion.get(Version.V_3_0_0));
        cumulativeEventStats.addAll(MinClusterVersionUtil.eventStatsByVersion.get(Version.V_3_1_0));
        assertEquals(eventStats, cumulativeEventStats);
    }
}
