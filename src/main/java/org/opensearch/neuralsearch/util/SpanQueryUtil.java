/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.spans.SpanOrQuery;
import org.apache.lucene.queries.spans.SpanQuery;
import org.apache.lucene.queries.spans.SpanTermQuery;
import org.apache.lucene.queryparser.xml.ParserException;
import org.apache.lucene.util.BytesRef;

public class SpanQueryUtil {
    public static SpanQuery getSpanOrTermQuery(Analyzer analyzer, String fieldName, String value) throws ParserException {
        List<SpanQuery> clausesList = new ArrayList<>();

        try (TokenStream ts = analyzer.tokenStream(fieldName, value)) {
            TermToBytesRefAttribute termAtt = ts.addAttribute(TermToBytesRefAttribute.class);
            ts.reset();
            while (ts.incrementToken()) {
                SpanTermQuery stq = new SpanTermQuery(new Term(fieldName, BytesRef.deepCopyOf(termAtt.getBytesRef())));
                clausesList.add(stq);
            }
            ts.end();
            return new SpanOrQuery(clausesList.toArray(new SpanQuery[clausesList.size()]));
        } catch (IOException ioe) {
            throw new ParserException("IOException parsing value:" + value, ioe);
        }
    }
}
