/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;
import org.opensearch.search.pipeline.PipelineConfiguration;
import org.opensearch.search.pipeline.SearchPipelineService;

/**
 * Resolves the resolver (fused) mode fusion config from the request's attached search pipeline, reproducing core's
 * pipeline-id resolution precedence (see {@code SearchPipelineService.resolvePipeline}). A fused-mode hybrid query can
 * read the SAME normalization/combination config an existing hybrid user already has in their pipeline, with no core
 * change and zero migration; an inline {@code fusion} block on the query body takes precedence over this (handled by
 * the caller via {@link FusionSpec#fromInlineFusion(Map)}).
 *
 * <p>The resolved pipeline id is never written back onto the request (core resolves it into a local variable), so the
 * plugin must re-derive it. Three definition sources, read from two surfaces, in core's exact order:
 * <ol>
 *   <li>Inline body {@code search_pipeline} block AND named {@code ?search_pipeline=} both present → hard error
 *       (mutually exclusive, not prioritized).</li>
 *   <li>Inline body block → read directly from the request source (an ad-hoc pipeline, never in cluster state).</li>
 *   <li>Named param → read that pipeline's config from cluster-state metadata by id.</li>
 *   <li>Index default ({@code index.search.default_pipeline}) → first concrete index's default wins; a second index
 *       with a different default collapses to {@code _none}.</li>
 *   <li>Else {@code _none} → no pipeline (returns null; the caller fails fast).</li>
 * </ol>
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
final class FusionConfigResolver {

    private static final String NONE_PIPELINE_ID = "_none";

    /**
     * Resolve the {@link FusionSpec} for a fused-mode request, or {@code null} when no pipeline / no fusion processor is
     * resolvable. Throws {@link IllegalArgumentException} when inline and named pipelines are both specified (mirrors
     * core).
     */
    static FusionSpec resolve(SearchRequest searchRequest) {
        Map<String, Object> inlineConfig = Objects.isNull(searchRequest.source()) ? null : searchRequest.source().searchPipelineSource();
        String namedPipeline = searchRequest.pipeline();

        // Step 1: inline body AND named param both present → hard error (core throws the same).
        if (Objects.nonNull(inlineConfig) && Objects.nonNull(namedPipeline)) {
            throw new IllegalArgumentException(
                "Both named and inline search pipeline were specified. Please only specify one or the other."
            );
        }

        // Step 2: inline body pipeline (ad-hoc; not in cluster state) → read straight off the request source.
        // CAVEAT (verified): core's resolvePipeline runs BEFORE query rewrite and builds the ad-hoc pipeline via
        // PipelineWithMetrics.create, whose ConfigurationUtils.readOptionalList(config, "phase_results_processors")
        // REMOVES the key from this same live map. So by the time this runs, searchPipelineSource() is typically
        // drained to {} and fromPipelineConfig returns null. Kept for completeness / body forms that survive, but the
        // inline-body path needs the same core fix as the URL param to be reliable.
        if (Objects.nonNull(inlineConfig)) {
            return FusionSpec.fromPipelineConfig(inlineConfig);
        }

        ClusterService clusterService = NeuralSearchClusterUtil.instance().getClusterService();
        if (Objects.isNull(clusterService)) {
            return null;
        }

        // Step 3: named ?search_pipeline= param → look up its config by id in cluster-state metadata.
        if (Objects.nonNull(namedPipeline)) {
            if (NONE_PIPELINE_ID.equals(namedPipeline)) {
                return null;
            }
            return FusionSpec.fromPipelineConfig(pipelineConfigById(clusterService, namedPipeline));
        }

        // Step 4: index default (index.search.default_pipeline), resolved per concrete index. First non-none default
        // wins; a second index declaring a DIFFERENT default collapses to _none (matches core).
        String indexDefaultId = resolveIndexDefaultPipelineId(searchRequest);
        if (Objects.isNull(indexDefaultId) || NONE_PIPELINE_ID.equals(indexDefaultId)) {
            return null;
        }
        return FusionSpec.fromPipelineConfig(pipelineConfigById(clusterService, indexDefaultId));
    }

    /** The config map of the search pipeline with the given id, or null when absent. */
    private static Map<String, Object> pipelineConfigById(ClusterService clusterService, String pipelineId) {
        List<PipelineConfiguration> pipelines = SearchPipelineService.getPipelines(clusterService.state(), pipelineId);
        if (Objects.isNull(pipelines) || pipelines.isEmpty()) {
            return null;
        }
        return pipelines.get(0).getConfigAsMap();
    }

    /**
     * Reproduce core's index-default resolution: scan the concrete indices' {@code index.search.default_pipeline}
     * settings — the first non-none default wins, but a second index with a different default collapses to
     * {@code _none} (no pipeline). Returns the resolved id, {@code _none}, or null when no index declares a default.
     */
    private static String resolveIndexDefaultPipelineId(SearchRequest searchRequest) {
        List<IndexMetadata> indices = NeuralSearchClusterUtil.instance().getIndexMetadataList(searchRequest);
        String pipelineId = null;
        for (IndexMetadata indexMetadata : indices) {
            if (Objects.isNull(indexMetadata)) {
                continue;
            }
            Settings indexSettings = indexMetadata.getSettings();
            if (IndexSettings.DEFAULT_SEARCH_PIPELINE.exists(indexSettings) == false) {
                continue;
            }
            String current = IndexSettings.DEFAULT_SEARCH_PIPELINE.get(indexSettings);
            if (Objects.isNull(pipelineId)) {
                pipelineId = current;
            } else if (pipelineId.equals(current) == false) {
                return NONE_PIPELINE_ID; // conflicting defaults across targeted indices → no pipeline
            }
        }
        return pipelineId;
    }
}
