/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.collapse;

import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.grouping.CollapseTopFieldDocs;
import org.apache.lucene.util.BytesRef;
import org.opensearch.neuralsearch.processor.CompoundTopDocs;
import org.opensearch.neuralsearch.search.query.HybridQueryFieldDocComparator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * A collector class that handles data collapsing operations for search results.
 * Supports collapsing on fields of type BytesRef or Long.
 *
 * @param <T> The type of the collapse field value (BytesRef or Long)
 */
@Log4j2
public class CollapseDataCollector<T> {

    private final Map<T, FieldDoc> collapseValueToTopDocMap = new HashMap<>();
    private final Map<T, Integer> collapseValueToShardMap = new HashMap<>();
    private final HybridQueryFieldDocComparator collapseComparator;
    private final Class<T> expectedType;
    @Getter
    private String collapseField;

    /**
     * Constructs a new CollapseDataCollector.
     *
     * @param collapseDTO The data transfer object containing collapse configuration and initial data
     * @throws IllegalArgumentException if the collapse value type cannot be determined from input data
     */
    public CollapseDataCollector(CollapseDTO collapseDTO) {
        this.collapseComparator = new HybridQueryFieldDocComparator(
            ((CollapseTopFieldDocs) collapseDTO.getCollapseQueryTopDocs()
                .get(collapseDTO.getIndexOfFirstNonEmpty())
                .getTopDocs()
                .getFirst()).fields,
            Comparator.comparing((ScoreDoc scoreDoc) -> scoreDoc.score)
        );

        this.expectedType = determineExpectedType(collapseDTO);
        if (this.expectedType == null) {
            log.error("Could not determine collapse value type from input data");
            throw new IllegalArgumentException("Could not determine collapse value type from input data");
        }
    }

    @SuppressWarnings("unchecked")
    private Class<T> determineExpectedType(CollapseDTO collapseDTO) {
        Object firstCollapseValue = ((CollapseTopFieldDocs) collapseDTO.getCollapseQueryTopDocs()
            .get(collapseDTO.getIndexOfFirstNonEmpty())
            .getTopDocs()
            .getFirst()).collapseValues[0];

        if (firstCollapseValue instanceof BytesRef) {
            return (Class<T>) BytesRef.class;
        } else if (firstCollapseValue instanceof Long) {
            return (Class<T>) Long.class;
        }
        return null;
    }

    /**
     * Processes and collects collapse data from search results across all shards.
     * Updates internal maps tracking the top documents and their corresponding shards for each collapse value.
     *
     * @param collapseDTO The data transfer object containing collapse query results
     * @throws IllegalArgumentException if the document structure doesn't match expected collapse format
     */
    public void collectCollapseData(CollapseDTO collapseDTO) {
        for (int shardIndex = 0; shardIndex < collapseDTO.getCollapseQuerySearchResults().size(); shardIndex++) {
            CompoundTopDocs updatedCollapseTopDocs = collapseDTO.getCollapseQueryTopDocs().get(shardIndex);
            List<ScoreDoc> updatedCollapseDocs = updatedCollapseTopDocs.getScoreDocs();

            if (updatedCollapseDocs.isEmpty()) {
                continue;
            }

            if (!(updatedCollapseTopDocs.getTopDocs().getFirst() instanceof CollapseTopFieldDocs)) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "Expected CollapseTopFieldDocs but got: %s",
                        updatedCollapseTopDocs.getTopDocs().getFirst().getClass().getSimpleName()
                    )
                );
            }

            collapseField = ((CollapseTopFieldDocs) updatedCollapseTopDocs.getTopDocs().getFirst()).field;

            for (int scoreDocIndex = 0; scoreDocIndex < updatedCollapseDocs.size(); scoreDocIndex++) {
                ScoreDoc scoreDoc = updatedCollapseDocs.get(scoreDocIndex);
                try {
                    processCollapseDoc(scoreDoc, shardIndex);
                } catch (ClassCastException | IllegalArgumentException e) {
                    log.error(String.format(Locale.ROOT, "Error processing collapse doc in shard %d: %s", shardIndex, e.getMessage()));
                    throw e;
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void processCollapseDoc(ScoreDoc scoreDoc, int shardIndex) {
        if (!(scoreDoc instanceof FieldDoc fieldDoc)) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Expected FieldDoc but got: %s", scoreDoc.getClass().getSimpleName())
            );
        }

        if (fieldDoc.fields == null || fieldDoc.fields.length == 0) {
            log.info("Field doc 'fields' attribute does not contain any values");
            return;
        }

        Object collapseValueObj = fieldDoc.fields[fieldDoc.fields.length - 1];
        if (collapseValueObj == null) {
            return;
        }

        if (!expectedType.isInstance(collapseValueObj)) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Expected collapse value of type %s but got: %s",
                    expectedType.getSimpleName(),
                    collapseValueObj.getClass().getSimpleName()
                )
            );
        }

        T collapseValue = (T) collapseValueObj;
        FieldDoc currentBestFieldDoc = collapseValueToTopDocMap.get(collapseValue);

        if (currentBestFieldDoc == null || collapseComparator.compare(fieldDoc, currentBestFieldDoc) < 0) {
            T key;
            if (collapseValue instanceof BytesRef) {
                key = (T) BytesRef.deepCopyOf((BytesRef) collapseValue);
            } else {
                key = collapseValue;
            }

            collapseValueToTopDocMap.put(key, fieldDoc);
            collapseValueToShardMap.put(key, shardIndex);
        }
    }

    /**
     * Returns a sorted list of collapse entries (collapse value to FieldDoc mappings).
     * The entries are sorted based on the collapse comparator.
     *
     * @return List of sorted Map.Entry objects containing collapse values and their corresponding top FieldDocs
     */
    public List<Map.Entry<T, FieldDoc>> getSortedCollapseEntries() {
        List<Map.Entry<T, FieldDoc>> collapseEntryList = new ArrayList<>(collapseValueToTopDocMap.entrySet());
        collapseEntryList.sort(Map.Entry.comparingByValue(collapseComparator));
        return collapseEntryList;
    }

    /**
     * Retrieves the shard index for a given collapse key.
     *
     * @param key The collapse value to look up
     * @return The shard index where the top document for this collapse value was found, or null if not found
     * @throws IllegalArgumentException if the provided key is not of the expected type
     */
    public Integer getCollapseShardIndex(T key) {
        if (!expectedType.isInstance(key)) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Expected key of type %s but got: %s",
                    expectedType.getSimpleName(),
                    key.getClass().getSimpleName()
                )
            );
        }
        return collapseValueToShardMap.get(key);
    }
}
