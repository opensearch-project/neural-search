/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import org.apache.lucene.codecs.Codec;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.codec.CodecServiceConfig;
import org.opensearch.index.mapper.MapperService;

public class SparseCodecService extends CodecService {
    private final MapperService mapperService;

    public SparseCodecService(CodecServiceConfig codecServiceConfig, IndexSettings indexSettings) {
        super(codecServiceConfig.getMapperService(), codecServiceConfig.getIndexSettings(), codecServiceConfig.getLogger());
        this.mapperService = codecServiceConfig.getMapperService();
    }

    @Override
    public Codec codec(String name) {
        return SparseCodec.builder().delegate(super.codec(name)).mapperService(mapperService).build();
    }
}
