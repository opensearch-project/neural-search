/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.NonNull;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.neuralsearch.sparse.cache.ClusteredPostingCache;
import org.opensearch.neuralsearch.sparse.common.MergeStateFacade;
import org.opensearch.neuralsearch.sparse.common.PredicateUtils;
import org.opensearch.neuralsearch.sparse.mapper.SparseVectorField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * This class is responsible for writing sparse postings to the index
 */
@Log4j2
public class SparsePostingsConsumer extends FieldsConsumer {
    private final FieldsConsumer delegate;
    private final SegmentWriteState state;
    private final MergeHelper mergeHelper;

    static final String CODEC_NAME = "SparsePostingsProducer";
    private final IndexOutput termsOut;
    private final IndexOutput postingOut;
    private final SparseTermsLuceneWriter sparseTermsLuceneWriter;

    // Initial format
    public static final int VERSION_START = 1;
    public static final int VERSION_CURRENT = VERSION_START;

    /** Extension of terms file */
    static final String TERMS_EXTENSION = "sit";
    static final String POSTING_EXTENSION = "sip";

    private final ClusteredPostingTermsWriter clusteredPostingTermsWriter;
    private long termsStartFp = 0;
    private long postingStartFp = 0;
    private boolean fromMerge = false;

    public SparsePostingsConsumer(@NonNull FieldsConsumer delegate, @NonNull SegmentWriteState state, @NonNull MergeHelper mergeHelper)
        throws IOException {
        this(
            delegate,
            state,
            mergeHelper,
            VERSION_CURRENT,
            new ClusteredPostingTermsWriter(CODEC_NAME, VERSION_CURRENT, new CodecUtilWrapper()),
            new SparseTermsLuceneWriter(CODEC_NAME, VERSION_CURRENT, new CodecUtilWrapper())
        );
    }

    public SparsePostingsConsumer(
        @NonNull FieldsConsumer delegate,
        @NonNull SegmentWriteState state,
        @NonNull MergeHelper mergeHelper,
        int version,
        ClusteredPostingTermsWriter termsWriter,
        SparseTermsLuceneWriter luceneWriter
    ) throws IOException {
        super();
        this.delegate = delegate;
        this.state = state;
        this.mergeHelper = mergeHelper;
        clusteredPostingTermsWriter = termsWriter;
        sparseTermsLuceneWriter = luceneWriter;

        final String termsFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, TERMS_EXTENSION);
        final String postingFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, POSTING_EXTENSION);

        boolean success = false;
        IndexOutput termsOut = null;
        IndexOutput postingOut = null;
        try {
            termsOut = state.directory.createOutput(termsFileName, state.context);
            sparseTermsLuceneWriter.init(termsOut, state);
            postingOut = state.directory.createOutput(postingFileName, state.context);
            clusteredPostingTermsWriter.init(postingOut, state);

            success = true;
        } finally {
            if (!success) {
                IOUtils.closeWhileHandlingException(termsOut, postingOut);
                this.termsOut = null;
                this.postingOut = null;
            } else {
                this.termsOut = termsOut;
                this.postingOut = postingOut;
                termsStartFp = termsOut.getFilePointer();
                postingStartFp = postingOut.getFilePointer();
            }
        }
    }

    private void writeSparseTerms(Fields fields, NormsProducer norms, List<String> sparseFields) throws IOException {
        this.sparseTermsLuceneWriter.writeFieldCount(sparseFields.size());

        String lastField = null;
        for (String field : sparseFields) {
            assert lastField == null || lastField.compareTo(field) < 0;
            lastField = field;
            this.sparseTermsLuceneWriter.writeFieldNumber(this.state.fieldInfos.fieldInfo(field).number);

            Terms terms = fields.terms(field);
            if (terms == null) {
                this.sparseTermsLuceneWriter.writeTermsSize(0);
                continue;
            }

            this.clusteredPostingTermsWriter.setFieldAndMaxDoc(
                this.state.fieldInfos.fieldInfo(field),
                this.state.segmentInfo.maxDoc(),
                false
            );

            TermsEnum termsEnum = terms.iterator();
            List<BytesRef> termsList = new ArrayList<>();
            List<BlockTermState> states = new ArrayList<>();
            while (true) {
                BytesRef term = termsEnum.next();
                if (term == null) {
                    break;
                }
                BytesRef clonedTerm = term.clone();
                BlockTermState state = this.clusteredPostingTermsWriter.write(clonedTerm, termsEnum, norms);
                termsList.add(clonedTerm);
                states.add(state);
            }
            this.sparseTermsLuceneWriter.writeTermsSize(termsList.size());
            for (int i = 0; i < termsList.size(); ++i) {
                this.sparseTermsLuceneWriter.writeTerm(termsList.get(i), states.get(i));
            }
        }
    }

    @Override
    public void write(Fields fields, NormsProducer norms) throws IOException {
        List<String> nonSparseFields = new ArrayList<>();
        List<String> sparseFields = new ArrayList<>();
        for (String field : fields) {
            FieldInfo fieldInfo = this.state.fieldInfos.fieldInfo(field);
            if (SparseVectorField.isSparseField(fieldInfo)
                && PredicateUtils.shouldRunSeisPredicate.test(this.state.segmentInfo, fieldInfo)) {
                sparseFields.add(field);
            } else {
                nonSparseFields.add(field);
            }
        }
        if (!nonSparseFields.isEmpty()) {
            Fields maskedFields = new FilterLeafReader.FilterFields(fields) {
                @Override
                public Iterator<String> iterator() {
                    return nonSparseFields.iterator();
                }
            };
            this.delegate.write(maskedFields, norms);
        }
        // if this is not a merge, write the sparse fields, if it's from merge, we handle it from merge()
        if (!this.fromMerge && !sparseFields.isEmpty()) {
            writeSparseTerms(fields, norms, sparseFields);
        }
    }

    @Override
    public void merge(MergeState mergeState, NormsProducer norms) throws IOException {
        this.fromMerge = true;
        // merge non-sparse fields
        super.merge(mergeState, norms);
        // merge sparse fields
        try {
            MergeStateFacade mergeStateFacade = mergeHelper.convertToMergeStateFacade(mergeState);
            SparsePostingsReader sparsePostingsReader = new SparsePostingsReader(mergeStateFacade, mergeHelper);
            sparsePostingsReader.merge(this.sparseTermsLuceneWriter, this.clusteredPostingTermsWriter);
            mergeHelper.clearCacheData(mergeStateFacade, null, ClusteredPostingCache.getInstance()::onIndexRemoval);
        } catch (Exception e) {
            log.error("Merge sparse postings error", e);
        }
    }

    @Override
    public void close() throws IOException {
        this.delegate.close();
        boolean success = false;
        try {
            this.sparseTermsLuceneWriter.close(this.termsStartFp);
            this.clusteredPostingTermsWriter.close(this.postingStartFp);
            success = true;
        } finally {
            if (success) {
                IOUtils.close(termsOut, postingOut);
            } else {
                IOUtils.closeWhileHandlingException(termsOut, postingOut);
            }
        }
    }
}
