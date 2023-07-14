/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.analyzer;

import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AbstractIndexAnalyzerProvider;
import org.opensearch.neuralsearch.index.analysis.BertAnalyzer;

import lombok.extern.log4j.Log4j2;

@Log4j2
public class BertAnalyzerProvider extends AbstractIndexAnalyzerProvider<BertAnalyzer> {
    private final BertAnalyzer analyzer;

    public BertAnalyzerProvider(
        IndexSettings indexSettings,
        Environment env,
        String name,
        Settings settings
    ) {
        super(indexSettings, name, settings);
        analyzer = new BertAnalyzer();
    }

    public static BertAnalyzerProvider getBertAnalyzerProvider(
        IndexSettings indexSettings,
        Environment env,
        String name,
        Settings settings
    ) {
        return new BertAnalyzerProvider(indexSettings, env, name, settings);
    }

    @Override
    public BertAnalyzer get() {
        return analyzer;
    }
}
