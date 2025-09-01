/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import org.apache.lucene.codecs.Codec;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.codec.CodecServiceConfig;

/**
 * It vends SparseCodec to engine to provide sparse vector codec.
 */
public class SparseCodecService extends CodecService {

    public SparseCodecService(CodecServiceConfig codecServiceConfig) {
        super(codecServiceConfig.getMapperService(), codecServiceConfig.getIndexSettings(), codecServiceConfig.getLogger());
    }

    @Override
    public Codec codec(String name) {
        return new SparseCodec(super.codec(name));
    }
}
