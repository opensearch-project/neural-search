/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.search.query;

import java.util.Comparator;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SortField;

/**
 * Comparator class that compares two field docs as per the sorting criteria
 */
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public class HybridQueryFieldDocComparator implements Comparator<FieldDoc> {
    final SortField[] sortFields;
    final FieldComparator<?>[] comparators;
    final int[] reverseMul;
    final Comparator<ScoreDoc> tieBreaker;

    public HybridQueryFieldDocComparator(SortField[] sortFields, Comparator<ScoreDoc> tieBreaker) {
        this.sortFields = sortFields;
        this.tieBreaker = tieBreaker;
        comparators = new FieldComparator[sortFields.length];
        reverseMul = new int[sortFields.length];
        for (int compIDX = 0; compIDX < sortFields.length; compIDX++) {
            final SortField sortField = sortFields[compIDX];
            comparators[compIDX] = sortField.getComparator(1, Pruning.NONE);
            reverseMul[compIDX] = sortField.getReverse() ? -1 : 1;
        }
    }

    @Override
    public int compare(final FieldDoc firstFD, final FieldDoc secondFD) {
        for (int compIDX = 0; compIDX < comparators.length; compIDX++) {
            final FieldComparator comp = comparators[compIDX];

            final int cmp = reverseMul[compIDX] * comp.compareValues(firstFD.fields[compIDX], secondFD.fields[compIDX]);

            if (cmp != 0) {
                return cmp;
            }
        }
        return tieBreakCompare(firstFD, secondFD, tieBreaker);
    }

    private int tieBreakCompare(ScoreDoc firstDoc, ScoreDoc secondDoc, Comparator<ScoreDoc> tieBreaker) {
        assert tieBreaker != null;
        int value = tieBreaker.compare(firstDoc, secondDoc);
        return value;
    }
}
