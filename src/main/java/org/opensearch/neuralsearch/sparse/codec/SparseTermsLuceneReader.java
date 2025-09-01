/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.extern.log4j.Log4j2;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.neuralsearch.sparse.data.DocWeight;
import org.opensearch.neuralsearch.sparse.data.DocumentCluster;
import org.opensearch.neuralsearch.sparse.data.PostingClusters;
import org.opensearch.neuralsearch.sparse.data.SparseVector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class read terms and clustered posting from lucene index.
 * It stores the posting to the cache data structure.
 */
@Log4j2
public class SparseTermsLuceneReader extends FieldsProducer {
    private final Map<String, Map<BytesRef, Long>> fieldToTerms = new HashMap<>();
    private IndexInput termsIn;
    private IndexInput postingIn;

    public SparseTermsLuceneReader(SegmentReadState state) {
        final String termsFileName = IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            SparsePostingsConsumer.TERMS_EXTENSION
        );
        final String postingFileName = IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            SparsePostingsConsumer.POSTING_EXTENSION
        );
        boolean success = false;
        try {
            termsIn = state.directory.openInput(termsFileName, state.context);
            CodecUtil.checkIndexHeader(
                termsIn,
                SparsePostingsConsumer.CODEC_NAME,
                SparsePostingsConsumer.VERSION_START,
                SparsePostingsConsumer.VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix
            );
            CodecUtil.retrieveChecksum(termsIn);
            seekDir(termsIn);

            postingIn = state.directory.openInput(postingFileName, state.context);
            CodecUtil.checkIndexHeader(
                postingIn,
                SparsePostingsConsumer.CODEC_NAME,
                SparsePostingsConsumer.VERSION_START,
                SparsePostingsConsumer.VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix
            );
            CodecUtil.retrieveChecksum(postingIn);

            int numberOfFields = termsIn.readVInt();
            for (int i = 0; i < numberOfFields; i++) {
                int fieldId = termsIn.readVInt();
                int numberOfTerms = (int) termsIn.readVLong();
                Map<BytesRef, Long> terms = new HashMap<>(numberOfTerms);
                for (int j = 0; j < numberOfTerms; j++) {
                    int byteLength = termsIn.readVInt();
                    BytesRef term = new BytesRef(byteLength);
                    term.length = byteLength;
                    try {
                        termsIn.readBytes(term.bytes, term.offset, byteLength);
                    } catch (Exception e) {
                        throw e;
                    }
                    long fileOffset = termsIn.readVLong();
                    terms.put(term, fileOffset);
                }
                fieldToTerms.put(state.fieldInfos.fieldInfo(fieldId).name, terms);
            }
            success = true;
        } catch (Exception e) {
            log.error("Read sparse terms error", e);
        } finally {
            if (!success) {
                IOUtils.closeWhileHandlingException(termsIn, postingIn);
            }
        }
    }

    private void seekDir(IndexInput input) throws IOException {
        input.seek(input.length() - CodecUtil.footerLength() - 8);
        long dirOffset = input.readLong();
        input.seek(dirOffset);
    }

    @Override
    public Iterator<String> iterator() {
        return fieldToTerms.keySet().iterator();
    }

    private synchronized List<DocumentCluster> readClusters(long offset) throws IOException {
        postingIn.seek(offset);
        long clusterSize = postingIn.readVLong();
        List<DocumentCluster> clusters = new ArrayList<>((int) clusterSize);
        for (int j = 0; j < clusterSize; j++) {
            long docSize = postingIn.readVLong();
            List<DocWeight> docs = new ArrayList<>((int) docSize);
            for (int k = 0; k < docSize; ++k) {
                docs.add(new DocWeight(postingIn.readVInt(), postingIn.readByte()));
            }
            boolean shouldNotSkip = postingIn.readByte() == 1;
            // summary
            long summaryVectorSize = postingIn.readVLong();
            List<SparseVector.Item> items = new ArrayList<>((int) summaryVectorSize);
            for (int k = 0; k < summaryVectorSize; ++k) {
                items.add(new SparseVector.Item(postingIn.readVInt(), postingIn.readByte()));
            }
            SparseVector summary = items.isEmpty() ? null : new SparseVector(items);
            DocumentCluster cluster = new DocumentCluster(summary, docs, shouldNotSkip);
            clusters.add(cluster);
        }
        return clusters;
    }

    @Override
    public Terms terms(String field) throws IOException {
        throw new UnsupportedOperationException();
    }

    public Set<BytesRef> getTerms(String field) {
        Map<BytesRef, Long> termsMapping = fieldToTerms.get(field);
        if (termsMapping == null) {
            return Set.of();
        }
        return termsMapping.keySet();
    }

    public PostingClusters read(String field, BytesRef term) throws IOException {
        Map<BytesRef, Long> termsMapping = fieldToTerms.get(field);
        if (termsMapping == null) {
            return null;
        }
        if (!termsMapping.containsKey(term)) {
            return null;
        }
        long offset = termsMapping.get(term);
        List<DocumentCluster> clusters = readClusters(offset);
        if (clusters.isEmpty()) {
            return null;
        }
        return new PostingClusters(clusters);
    }

    @Override
    public int size() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(this.termsIn, this.postingIn);
    }

    @Override
    public void checkIntegrity() throws IOException {
        CodecUtil.checksumEntireFile(termsIn);
        CodecUtil.checksumEntireFile(postingIn);
    }
}
