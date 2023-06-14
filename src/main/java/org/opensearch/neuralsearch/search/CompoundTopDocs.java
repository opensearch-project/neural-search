/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.search;

import java.util.Arrays;

import lombok.Getter;
import lombok.ToString;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;

/**
 * Class stores collection of TodDocs for each sub query from hybrid query
 */
@ToString(includeFieldNames = true)
public class CompoundTopDocs extends TopDocs {

    @Getter
    private TopDocs[] compoundTopDocs;

    public CompoundTopDocs(TotalHits totalHits, ScoreDoc[] scoreDocs) {
        super(totalHits, scoreDocs);
    }

    public CompoundTopDocs(TotalHits totalHits, TopDocs[] docs) {
        // we pass clone of score docs from the sub-query that has most hits
        super(totalHits, cloneLargestScoreDocs(docs));
        this.compoundTopDocs = docs;
    }

    private static ScoreDoc[] cloneLargestScoreDocs(TopDocs[] docs) {
        if (docs == null) {
            return null;
        }
        ScoreDoc[] maxScoreDocs = null;
        int maxLength = -1;
        for (TopDocs topDoc : docs) {
            if (topDoc == null || topDoc.scoreDocs == null) {
                continue;
            }
            if (topDoc.scoreDocs.length > maxLength) {
                maxLength = topDoc.scoreDocs.length;
                maxScoreDocs = topDoc.scoreDocs;
            }
        }
        if (maxScoreDocs == null) {
            return null;
        }
        // do deep copy
        return Arrays.stream(maxScoreDocs).map(doc -> new ScoreDoc(doc.doc, doc.score, doc.shardIndex)).toArray(ScoreDoc[]::new);
    }
}
