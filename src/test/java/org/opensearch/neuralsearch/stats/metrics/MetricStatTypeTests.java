/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.stats.metrics;

import org.opensearch.test.OpenSearchTestCase;

import java.util.EnumSet;

public class MetricStatTypeTests extends OpenSearchTestCase {

    public void testGetTypeString() {
        EnumSet<MetricStatName> metricStatNames = EnumSet.allOf(MetricStatName.class);
        for (MetricStatName metricStatName : metricStatNames) {
            assertEquals("memory", metricStatName.getStatType().getTypeString());
        }
    }
}
