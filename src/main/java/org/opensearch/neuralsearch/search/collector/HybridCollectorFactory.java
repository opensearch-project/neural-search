/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.collector;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.neuralsearch.search.HitsThresholdChecker;
import org.opensearch.search.collapse.CollapseContext;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.sort.SortAndFormats;

import java.util.Locale;

import static org.opensearch.neuralsearch.settings.NeuralSearchSettings.HYBRID_COLLAPSE_DOCS_PER_GROUP_PER_SUBQUERY;

/**
 * A factory class for creating various types of Hybrid Collectors based on the provided configuration.
 */
public class HybridCollectorFactory {

    /**
     * Creates and returns a Collector based on the configuration provided in the HybridCollectorFactoryDTO.
     *
     * @param hybridCollectorFactoryDTO A data transfer object containing the configuration for creating the collector.
     * @return A Collector instance based on the provided configuration.
     * @throws IllegalStateException If an unsupported field type is provided for collapsing.
     * @throws IllegalArgumentException If the search_after configuration is invalid.
     */
    public static Collector createCollector(HybridCollectorFactoryDTO hybridCollectorFactoryDTO) {
        SortAndFormats sortAndFormats = hybridCollectorFactoryDTO.getSortAndFormats();
        SearchContext searchContext = hybridCollectorFactoryDTO.getSearchContext();
        CollapseContext collapseContext = searchContext.collapse();
        HitsThresholdChecker hitsThresholdChecker = hybridCollectorFactoryDTO.getHitsThresholdChecker();
        int numHits = hybridCollectorFactoryDTO.getNumHits();
        FieldDoc after = hybridCollectorFactoryDTO.getAfter();
        if (collapseContext != null) {
            // Collapse is applied
            int docsPerGroupPerSubquery = HYBRID_COLLAPSE_DOCS_PER_GROUP_PER_SUBQUERY.get(
                searchContext.indexShard().indexSettings().getSettings()
            );
            MappedFieldType fieldType = collapseContext.getFieldType();
            if (fieldType instanceof KeywordFieldMapper.KeywordFieldType) {
                return HybridCollapsingTopDocsCollector.createKeyword(
                    collapseContext.getFieldName(),
                    fieldType,
                    sortAndFormats == null ? new Sort(new SortField(null, SortField.Type.SCORE)) : sortAndFormats.sort,
                    numHits,
                    hitsThresholdChecker,
                    docsPerGroupPerSubquery
                );
            } else if (fieldType instanceof NumberFieldMapper.NumberFieldType) {
                return HybridCollapsingTopDocsCollector.createNumeric(
                    collapseContext.getFieldName(),
                    fieldType,
                    sortAndFormats == null ? new Sort(new SortField(null, SortField.Type.SCORE)) : sortAndFormats.sort,
                    numHits,
                    hitsThresholdChecker,
                    docsPerGroupPerSubquery
                );
            } else {
                throw new IllegalStateException(
                    String.format(
                        Locale.ROOT,
                        "unknown type for collapse field %s, only keywords and numbers are accepted",
                        collapseContext.getFieldName()
                    )
                );
            }
        } else {
            if (sortAndFormats == null) {
                return new HybridTopScoreDocCollector(numHits, hitsThresholdChecker);
            } else {
                // Sorting is applied
                if (after == null) {
                    return new SimpleFieldCollector(numHits, hitsThresholdChecker, sortAndFormats.sort);
                } else {
                    // search_after is applied
                    validateSearchAfterFieldAndSortFormats(sortAndFormats, after);
                    return new PagingFieldCollector(numHits, hitsThresholdChecker, sortAndFormats.sort, after);
                }
            }
        }
    }

    private static void validateSearchAfterFieldAndSortFormats(SortAndFormats sortAndFormats, FieldDoc after) {
        if (after.fields == null) {
            throw new IllegalArgumentException("after.fields wasn't set; you must pass fillFields=true for the previous search");
        }

        if (after.fields.length != sortAndFormats.sort.getSort().length) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "after.fields has %s values but sort has %s",
                    after.fields.length,
                    sortAndFormats.sort.getSort().length
                )
            );
        }
    }
}
