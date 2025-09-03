/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.Codec;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.codec.CodecServiceConfig;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.neuralsearch.sparse.AbstractSparseTestBase;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SparseCodecServiceTests extends AbstractSparseTestBase {

    @Mock
    private MapperService mockMapperService;

    @Mock
    private IndexSettings mockIndexSettings;

    @Mock
    private Logger mockLogger;

    @Mock
    private CodecServiceConfig mockCodecServiceConfig;

    @Before
    @Override
    public void setUp() {
        super.setUp();
        MockitoAnnotations.openMocks(this);

        when(mockCodecServiceConfig.getMapperService()).thenReturn(mockMapperService);
        when(mockCodecServiceConfig.getIndexSettings()).thenReturn(mockIndexSettings);
        when(mockCodecServiceConfig.getLogger()).thenReturn(mockLogger);
    }

    public void testConstructor() {
        SparseCodecService service = new SparseCodecService(mockCodecServiceConfig);

        assertNotNull(service);
        verify(mockCodecServiceConfig, times(1)).getMapperService();
        verify(mockCodecServiceConfig, times(1)).getIndexSettings();
        verify(mockCodecServiceConfig, times(1)).getLogger();
    }

    public void testCodec() {
        String codecName = CodecService.DEFAULT_CODEC;

        SparseCodecService service = new SparseCodecService(mockCodecServiceConfig);
        Codec result = service.codec(codecName);

        assertNotNull(result);
        assertTrue(result instanceof SparseCodec);
    }
}
