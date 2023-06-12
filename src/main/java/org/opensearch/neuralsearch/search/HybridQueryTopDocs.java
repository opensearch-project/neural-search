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
public class HybridQueryTopDocs extends TopDocs {

    @Getter
    private TopDocs[] hybridQueryTopdDocs;

    public HybridQueryTopDocs(TotalHits totalHits, ScoreDoc[] scoreDocs) {
        super(totalHits, scoreDocs);
    }

    public HybridQueryTopDocs(TotalHits totalHits, TopDocs[] docs) {
        super(totalHits, cloneScoreDocs(docs[0].scoreDocs));
        this.hybridQueryTopdDocs = docs;
    }

    private static ScoreDoc[] cloneScoreDocs(ScoreDoc[] original) {
        if (original == null) {
            return null;
        }
        // do deep copy
        return Arrays.stream(original).map(doc -> new ScoreDoc(doc.doc, doc.score, doc.shardIndex)).toArray(ScoreDoc[]::new);
    }
}
