/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PushPostingsWriterBase;
import org.apache.lucene.backward_codecs.lucene101.Lucene101PostingsFormat;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.neuralsearch.sparse.accessor.ClusteredPostingWriter;
import org.opensearch.neuralsearch.sparse.accessor.SparseVectorForwardIndex;
import org.opensearch.neuralsearch.sparse.algorithm.seismic.ClusteringTask;
import org.opensearch.neuralsearch.sparse.algorithm.seismic.RandomClusteringAlgorithm;
import org.opensearch.neuralsearch.sparse.algorithm.seismic.SeismicPostingClusterer;
import org.opensearch.neuralsearch.sparse.cache.CacheGatedForwardIndexReader;
import org.opensearch.neuralsearch.sparse.cache.CacheKey;
import org.opensearch.neuralsearch.sparse.cache.ClusteredPostingCache;
import org.opensearch.neuralsearch.sparse.cache.ForwardIndexCache;
import org.opensearch.neuralsearch.sparse.common.IteratorWrapper;
import org.opensearch.neuralsearch.sparse.common.ValueEncoder;
import org.opensearch.neuralsearch.sparse.data.DocWeight;
import org.opensearch.neuralsearch.sparse.data.DocumentCluster;
import org.opensearch.neuralsearch.sparse.data.PostingClusters;
import org.opensearch.neuralsearch.sparse.data.SparseVector;
import org.opensearch.neuralsearch.sparse.quantization.ByteQuantizer;
import org.opensearch.neuralsearch.sparse.quantization.ByteQuantizerUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.opensearch.neuralsearch.sparse.common.SparseConstants.CLUSTER_RATIO_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.N_POSTINGS_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.SUMMARY_PRUNE_RATIO_FIELD;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.Seismic.DEFAULT_N_POSTINGS;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.Seismic.DEFAULT_POSTING_MINIMUM_LENGTH;
import static org.opensearch.neuralsearch.sparse.common.SparseConstants.Seismic.DEFAULT_POSTING_PRUNE_RATIO;

/**
 * ClusteredPostingTermsWriter is used to write postings for each segment.
 * It handles the logic to write data to both cache and lucene index.
 */
@Log4j2
@RequiredArgsConstructor
public class ClusteredPostingTermsWriter extends PushPostingsWriterBase {
    private FixedBitSet docsSeen;
    private IndexOutput postingOut;
    private final List<DocWeight> docWeights = new ArrayList<>();
    private BytesRef currentTerm;
    private SeismicPostingClusterer seismicPostingClusterer;
    private CacheKey key;
    private final String codecName;
    private final int version;
    private SegmentWriteState state;
    private DocValuesProducer docValuesProducer;
    private final CodecUtilWrapper codecUtilWrapper;
    private ByteQuantizer byteQuantizer;

    @Override
    public void setField(FieldInfo fieldInfo) {
        super.setField(fieldInfo);
        byteQuantizer = ByteQuantizerUtil.getByteQuantizerIngest(fieldInfo);
    }

    public BlockTermState write(BytesRef text, TermsEnum termsEnum, NormsProducer norms) throws IOException {
        this.currentTerm = text;
        return super.writeTerm(text, termsEnum, docsSeen, norms);
    }

    public BlockTermState write(BytesRef text, PostingClusters postingClusters) throws IOException {
        this.currentTerm = text;
        BlockTermState state = newTermState();
        writePostingClusters(postingClusters, state);
        return state;
    }

    public void setFieldAndMaxDoc(FieldInfo fieldInfo, int maxDoc, boolean isMerge) {
        setField(fieldInfo);
        key = new CacheKey(this.state.segmentInfo, fieldInfo);

        if (!isMerge) {
            setSeismicPostingClusterer(maxDoc);
        }
    }

    @Override
    public BlockTermState newTermState() throws IOException {
        return new Lucene101PostingsFormat.IntBlockTermState();
    }

    @Override
    public void startTerm(NumericDocValues norms) throws IOException {
        docWeights.clear();
    }

    private void setSeismicPostingClusterer(int maxDoc) {
        SparseVectorForwardIndex index = ForwardIndexCache.getInstance().getOrCreate(key, maxDoc);

        SparseBinaryDocValuesPassThrough luceneReader = null;
        DocValuesFormat fmt = this.state.segmentInfo.getCodec().docValuesFormat();
        SegmentReadState readState = new SegmentReadState(
            this.state.directory,
            this.state.segmentInfo,
            this.state.fieldInfos,
            IOContext.DEFAULT
        );
        try {
            this.docValuesProducer = fmt.fieldsProducer(readState);
            BinaryDocValues binaryDocValues = this.docValuesProducer.getBinary(fieldInfo);
            if (binaryDocValues != null) {
                luceneReader = new SparseBinaryDocValuesPassThrough(binaryDocValues, this.state.segmentInfo, byteQuantizer);
            }
        } catch (Exception e) {
            log.error("Failed to retrieve lucene reader");
        }

        float clusterRatio = Float.parseFloat(fieldInfo.attributes().get(CLUSTER_RATIO_FIELD));
        int nPostings;
        if (Integer.parseInt(fieldInfo.attributes().get(N_POSTINGS_FIELD)) == DEFAULT_N_POSTINGS) {
            nPostings = Math.max((int) (DEFAULT_POSTING_PRUNE_RATIO * maxDoc), DEFAULT_POSTING_MINIMUM_LENGTH);
        } else {
            nPostings = Integer.parseInt(fieldInfo.attributes().get(N_POSTINGS_FIELD));
        }
        float summaryPruneRatio = Float.parseFloat(fieldInfo.attributes().get(SUMMARY_PRUNE_RATIO_FIELD));

        this.seismicPostingClusterer = new SeismicPostingClusterer(
            nPostings,
            new RandomClusteringAlgorithm(
                summaryPruneRatio,
                clusterRatio,
                new CacheGatedForwardIndexReader(index.getReader(), index.getWriter(), luceneReader)
            )
        );
    }

    private void writePostingClusters(PostingClusters postingClusters, BlockTermState state) throws IOException {
        List<DocumentCluster> clusters = postingClusters.getClusters();
        // write file
        state.blockFilePointer = postingOut.getFilePointer();
        postingOut.writeVLong(clusters.size());
        for (DocumentCluster cluster : clusters) {
            postingOut.writeVLong(cluster.size());
            Iterator<DocWeight> iterator = cluster.iterator();
            while (iterator.hasNext()) {
                DocWeight docWeight = iterator.next();
                postingOut.writeVInt(docWeight.getDocID());
                postingOut.writeByte(docWeight.getWeight());
            }
            postingOut.writeByte((byte) (cluster.isShouldNotSkip() ? 1 : 0));
            if (cluster.getSummary() == null) {
                postingOut.writeVLong(0);
            } else {
                IteratorWrapper<SparseVector.Item> iter = cluster.getSummary().iterator();
                postingOut.writeVLong(cluster.getSummary().getSize());
                while (iter.hasNext()) {
                    SparseVector.Item item = iter.next();
                    postingOut.writeVInt(item.getToken());
                    postingOut.writeByte(item.getWeight());
                }
            }
        }
    }

    @Override
    public void finishTerm(BlockTermState state) throws IOException {
        ClusteredPostingWriter writer = ClusteredPostingCache.getInstance().getOrCreate(key).getWriter();
        PostingClusters postingClusters = new ClusteringTask(this.currentTerm, docWeights, writer, this.seismicPostingClusterer).get();
        writePostingClusters(postingClusters, state);
        this.docWeights.clear();
        this.currentTerm = null;
    }

    @Override
    public void startDoc(int docID, int freq) throws IOException {
        if (docID == -1) {
            throw new IllegalStateException("docId must be set before startDoc");
        }
        docWeights.add(new DocWeight(docID, byteQuantizer.quantize(ValueEncoder.decodeFeatureValue(freq))));
    }

    @Override
    public void addPosition(int position, BytesRef payload, int startOffset, int endOffset) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void finishDoc() throws IOException {
        // we don't have logic around finishing docs
    }

    @Override
    public void init(IndexOutput termsOut, SegmentWriteState state) throws IOException {
        this.postingOut = termsOut;
        this.state = state;
        this.docsSeen = new FixedBitSet(state.segmentInfo.maxDoc());
        this.codecUtilWrapper.writeIndexHeader(postingOut, this.codecName, version, state.segmentInfo.getId(), state.segmentSuffix);
    }

    @Override
    public void encodeTerm(DataOutput out, FieldInfo fieldInfo, BlockTermState state, boolean absolute) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        this.codecUtilWrapper.writeFooter(this.postingOut);
        if (this.docValuesProducer != null) {
            this.docValuesProducer.close();
            this.docValuesProducer = null;
        }
    }

    public void closeWithException() {
        IOUtils.closeWhileHandlingException(this.postingOut);
        if (this.docValuesProducer != null) {
            IOUtils.closeWhileHandlingException(this.docValuesProducer);
            this.docValuesProducer = null;
        }
    }

    public void close(long startFp) throws IOException {
        this.postingOut.writeLong(startFp);
        close();
    }
}
