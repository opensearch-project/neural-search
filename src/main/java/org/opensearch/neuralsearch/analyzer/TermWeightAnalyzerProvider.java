/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.analyzer;

import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AbstractIndexAnalyzerProvider;
import org.opensearch.neuralsearch.index.analysis.TermWeightAnalyzer;

import lombok.extern.log4j.Log4j2;

@Log4j2
public class TermWeightAnalyzerProvider extends AbstractIndexAnalyzerProvider<TermWeightAnalyzer> {
    private final TermWeightAnalyzer analyzer;

    public TermWeightAnalyzerProvider(
        IndexSettings indexSettings,
        Environment env,
        String name,
        Settings settings
    ) {
        super(indexSettings, name, settings);
        analyzer = new TermWeightAnalyzer();
    }

    public static TermWeightAnalyzerProvider geTermWeightAnalyzerProvider(
        IndexSettings indexSettings,
        Environment env,
        String name,
        Settings settings
    ) {
        return new TermWeightAnalyzerProvider(indexSettings, env, name, settings);
    }

    @Override
    public TermWeightAnalyzer get() {
        return analyzer;
    }
    
}
