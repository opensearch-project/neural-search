/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.search;

import lombok.Getter;
import lombok.ToString;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;

/**
 * Class stores collection of TodDocs for each sub query from hybrid query
 */
@ToString(includeFieldNames = true)
public class HybridQueryTopDocs extends TopDocs {

    @Getter
    TopDocs[] hybridQueryTopdDocs;

    public HybridQueryTopDocs(TotalHits totalHits, ScoreDoc[] scoreDocs) {
        super(totalHits, scoreDocs);
    }

    public HybridQueryTopDocs(TotalHits totalHits, TopDocs[] docs) {
        super(totalHits, copyScoreDocs(docs[0].scoreDocs));
        this.hybridQueryTopdDocs = docs;
    }

    private static ScoreDoc[] copyScoreDocs(ScoreDoc[] original) {
        if (original == null) {
            return null;
        }
        // do deep copy
        ScoreDoc[] copy = new ScoreDoc[original.length];
        for (int i = 0; i < original.length; i++) {
            ScoreDoc oneOriginalDoc = original[i];
            copy[i] = new ScoreDoc(oneOriginalDoc.doc, oneOriginalDoc.score, oneOriginalDoc.shardIndex);
        }
        return copy;
    }
}
