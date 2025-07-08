/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm;

import lombok.extern.log4j.Log4j2;
import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.sparse.codec.ClusteredPostingWriter;
import org.opensearch.neuralsearch.sparse.codec.InMemoryClusteredPosting;
import org.opensearch.neuralsearch.sparse.common.DocFreq;
import org.opensearch.neuralsearch.sparse.common.InMemoryKey;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

@Log4j2
public class ClusteringTask implements Supplier<PostingClusters> {
    private final BytesRef term;
    private final List<DocFreq> docs;
    private final PostingClustering postingClustering;
    private final InMemoryKey.IndexKey key;

    public ClusteringTask(BytesRef term, Collection<DocFreq> docs, InMemoryKey.IndexKey key, PostingClustering postingClustering) {
        this.docs = docs.stream().toList();
        this.term = BytesRef.deepCopyOf(term);
        this.key = key;
        this.postingClustering = postingClustering;
    }

    @Override
    public PostingClusters get() {
        List<DocumentCluster> clusters = null;
        try {
            clusters = postingClustering.cluster(this.docs);
        } catch (IOException e) {
            log.error("cluster failed", e);
            throw new RuntimeException(e);
        }
        ClusteredPostingWriter writer = InMemoryClusteredPosting.getOrCreate(key).getWriter();
        writer.write(term, clusters);
        return new PostingClusters(clusters);
    }
}
