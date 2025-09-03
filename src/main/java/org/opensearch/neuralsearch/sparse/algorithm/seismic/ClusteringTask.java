/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.algorithm.seismic;

import lombok.extern.log4j.Log4j2;
import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.sparse.accessor.ClusteredPostingWriter;
import org.opensearch.neuralsearch.sparse.data.DocWeight;
import org.opensearch.neuralsearch.sparse.data.DocumentCluster;
import org.opensearch.neuralsearch.sparse.data.PostingClusters;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

@Log4j2
public class ClusteringTask implements Supplier<PostingClusters> {
    private final BytesRef term;
    private final List<DocWeight> docs;
    private final SeismicPostingClusterer seismicPostingClusterer;
    private final ClusteredPostingWriter writer;

    public ClusteringTask(
        BytesRef term,
        Collection<DocWeight> docs,
        ClusteredPostingWriter writer,
        SeismicPostingClusterer seismicPostingClusterer
    ) {
        this.docs = docs.stream().toList();
        this.term = BytesRef.deepCopyOf(term);
        this.writer = writer;
        this.seismicPostingClusterer = seismicPostingClusterer;
    }

    @Override
    public PostingClusters get() {
        List<DocumentCluster> clusters;
        try {
            clusters = seismicPostingClusterer.cluster(this.docs);
        } catch (IOException e) {
            log.error("cluster failed", e);
            throw new RuntimeException(e);
        }
        writer.insert(term, clusters);
        return new PostingClusters(clusters);
    }
}
