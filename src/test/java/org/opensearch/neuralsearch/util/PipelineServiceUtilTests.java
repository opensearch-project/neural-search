/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.util;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class PipelineServiceUtilTests extends OpenSearchTestCase {
    public void test_getIngestPipelineConfigs_returnsEmptyList() {
        ClusterService mockClusterService = mock(ClusterService.class);
        PipelineServiceUtil utilSpy = spy(new PipelineServiceUtil(mockClusterService));

        doReturn(Collections.emptyList()).when(utilSpy).getIngestPipelines();

        List<Map<String, Object>> configs = utilSpy.getIngestPipelineConfigs();

        verify(utilSpy, times(1)).getIngestPipelines();
        assertTrue(configs.isEmpty());
    }

    public void test_getSearchPipelineConfigs_returnsEmptyList() {
        ClusterService mockClusterService = mock(ClusterService.class);
        PipelineServiceUtil utilSpy = spy(new PipelineServiceUtil(mockClusterService));

        doReturn(Collections.emptyList()).when(utilSpy).getSearchPipelines();

        List<Map<String, Object>> configs = utilSpy.getSearchPipelineConfigs();

        verify(utilSpy, times(1)).getSearchPipelines();
        assertTrue(configs.isEmpty());
    }
}
