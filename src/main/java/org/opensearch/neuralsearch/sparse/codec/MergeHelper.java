/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.Nullable;
import org.opensearch.neuralsearch.sparse.cache.CacheKey;
import org.opensearch.neuralsearch.sparse.common.MergeStateFacade;
import org.opensearch.neuralsearch.sparse.common.ValueEncoder;
import org.opensearch.neuralsearch.sparse.data.DocWeight;
import org.opensearch.neuralsearch.sparse.mapper.SparseVectorField;
import org.opensearch.neuralsearch.sparse.quantization.ByteQuantizer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Helper class for managing cache data during segment merges in sparse vector fields.
 */
@NoArgsConstructor
@Log4j2
public class MergeHelper {
    /**
     * Clears cache data for sparse vector fields during segment merge operations.
     *
     * @param mergeStateFacade the merge state containing doc values producers and field info
     * @param fieldInfo specific field to process, or null to process all sparse fields
     * @param consumer callback to handle cache key removal
     * @throws IOException if doc values cannot be accessed
     */
    public void clearCacheData(
        @NonNull MergeStateFacade mergeStateFacade,
        @Nullable FieldInfo fieldInfo,
        @NonNull Consumer<CacheKey> consumer
    ) throws IOException {
        for (DocValuesProducer producer : mergeStateFacade.getDocValuesProducers()) {
            for (FieldInfo field : mergeStateFacade.getMergeFieldInfos()) {
                boolean isNotSparse = !SparseVectorField.isSparseField(field);
                boolean fieldInfoMisMatched = fieldInfo != null && field != fieldInfo;
                if (isNotSparse || fieldInfoMisMatched) {
                    continue;
                }
                BinaryDocValues binaryDocValues = producer.getBinary(field);
                if (!(binaryDocValues instanceof SparseBinaryDocValuesPassThrough binaryDocValuesPassThrough)) {
                    continue;
                }
                CacheKey key = new CacheKey(binaryDocValuesPassThrough.getSegmentInfo(), field);
                consumer.accept(key);
            }
        }
    }

    /**
     * Retrieves merged posting list for a specific term across all segments.
     *
     * @param mergeStateFacade merge state containing producers and doc maps
     * @param term the term to retrieve postings for
     * @param fieldInfo field information for the sparse vector field
     * @param newIdToFieldProducerIndex array to store field producer index for each new doc ID
     * @param newIdToOldId array to store old doc ID mapping for each new doc ID
     * @return list of document weights for the term
     * @throws IOException if postings cannot be accessed
     */
    public List<DocWeight> getMergedPostingForATerm(
        MergeStateFacade mergeStateFacade,
        BytesRef term,
        FieldInfo fieldInfo,
        int[] newIdToFieldProducerIndex,
        int[] newIdToOldId
    ) throws IOException {
        List<DocWeight> docWeights = new ArrayList<>();
        for (int i = 0; i < mergeStateFacade.getFieldsProducers().length; i++) {
            // we need this SparseBinaryDocValuesPassThrough to get segment info
            BinaryDocValues binaryDocValues = mergeStateFacade.getDocValuesProducers()[i].getBinary(fieldInfo);
            if (!(binaryDocValues instanceof SparseBinaryDocValuesPassThrough)) {
                continue;
            }
            FieldsProducer fieldsProducer = mergeStateFacade.getFieldsProducers()[i];
            Terms terms = fieldsProducer.terms(fieldInfo.getName());
            if (terms == null) {
                continue;
            }

            TermsEnum termsEnum = terms.iterator();
            if (termsEnum == null) {
                continue;
            }

            if (!termsEnum.seekExact(term)) {
                continue;
            }
            PostingsEnum postings = termsEnum.postings(null);
            if (postings == null) {
                continue;
            }
            boolean isSparsePostings = postings instanceof SparsePostingsEnum;
            int docId = postings.nextDoc();
            for (; docId != PostingsEnum.NO_MORE_DOCS; docId = postings.nextDoc()) {
                if (docId == -1) {
                    continue;
                }
                int newDocId = mergeStateFacade.getDocMaps()[i].get(docId);
                if (newDocId == -1) {
                    continue;
                }
                if (newDocId >= newIdToFieldProducerIndex.length) {
                    throw new IndexOutOfBoundsException("newDocId is larger than array size!");
                }
                newIdToFieldProducerIndex[newDocId] = i;
                newIdToOldId[newDocId] = docId;
                int freq = postings.freq();
                byte freqByte = 0;
                if (isSparsePostings) {
                    // SparsePostingsEnum.freq() already transform byte freq to int
                    freqByte = (byte) freq;
                } else {
                    // decode to float first
                    freqByte = ByteQuantizer.quantizeFloatToByte(ValueEncoder.decodeFeatureValue(freq));
                }
                docWeights.add(new DocWeight(newDocId, freqByte));
            }
        }
        return docWeights;
    }

    /**
     * Collects all unique terms from segments being merged.
     *
     * @param mergeStateFacade merge state containing field producers
     * @param fieldInfo field information for the sparse vector field
     * @return set of all unique terms across segments
     * @throws IOException if terms cannot be accessed
     */
    public Set<BytesRef> getAllTerms(MergeStateFacade mergeStateFacade, FieldInfo fieldInfo) throws IOException {
        Set<BytesRef> allTerms = new HashSet<>();
        for (int i = 0; i < mergeStateFacade.getFieldsProducers().length; i++) {
            FieldsProducer fieldsProducer = mergeStateFacade.getFieldsProducers()[i];
            // we need this SparseBinaryDocValuesPassThrough to get segment info
            BinaryDocValues binaryDocValues = mergeStateFacade.getDocValuesProducers()[i].getBinary(fieldInfo);
            if (!(binaryDocValues instanceof SparseBinaryDocValuesPassThrough)) {
                continue;
            }

            Terms terms = fieldsProducer.terms(fieldInfo.getName());
            if (terms instanceof SparseTerms sparseTerms) {
                allTerms.addAll(sparseTerms.getReader().getTerms());
            } else {
                // fieldsProducer could be a delegate one as we need to merge normal segments into seis segment
                TermsEnum termsEnum = terms.iterator();
                while (true) {
                    BytesRef term = termsEnum.next();
                    if (term == null) {
                        break;
                    }
                    allTerms.add(BytesRef.deepCopyOf(term));
                }
            }
        }
        return allTerms;
    }

    /**
     * A helper function to create merge state facade which enables better testability.
     * @param mergeState {@link MergeState}
     * @return {@link MergeStateFacade}
     */
    public MergeStateFacade convertToMergeStateFacade(MergeState mergeState) {
        return new MergeStateFacade(mergeState);
    }

    /**
     * Create a new SparseDocValuesReader instance
     * @param mergeStateFacade {@link MergeStateFacade}
     * @return {@link SparseDocValuesReader}
     */
    public SparseDocValuesReader newSparseDocValuesReader(MergeStateFacade mergeStateFacade) {
        return new SparseDocValuesReader(mergeStateFacade);
    }
}
