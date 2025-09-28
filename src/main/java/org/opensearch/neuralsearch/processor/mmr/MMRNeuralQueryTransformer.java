/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.mmr;

import lombok.NoArgsConstructor;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.core.action.ActionListener;

import org.opensearch.knn.index.mapper.KNNVectorFieldMapper;
import org.opensearch.knn.search.processor.mmr.MMRQueryTransformer;
import org.opensearch.knn.search.processor.mmr.MMRRerankContext;
import org.opensearch.knn.search.processor.mmr.MMRTransformContext;
import org.opensearch.knn.search.processor.mmr.MMRVectorFieldInfo;
import org.opensearch.neuralsearch.mapper.SemanticFieldMapper;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;
import org.opensearch.neuralsearch.stats.events.EventStatName;
import org.opensearch.neuralsearch.stats.events.EventStatsManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.opensearch.knn.common.KNNConstants.TYPE;
import static org.opensearch.knn.common.KNNConstants.VECTOR_FIELD_PATH;
import static org.opensearch.knn.search.processor.mmr.MMRUtil.getMMRFieldMappingByPath;
import static org.opensearch.knn.search.processor.mmr.MMRUtil.resolveKnnVectorFieldInfo;
import static org.opensearch.neuralsearch.constants.MappingConstants.PATH_SEPARATOR;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.CHUNKING;
import static org.opensearch.neuralsearch.constants.SemanticInfoFieldConstants.EMBEDDING_FIELD_NAME;
import static org.opensearch.neuralsearch.util.SemanticMappingUtils.getSemanticInfoFieldFullPath;

@NoArgsConstructor
public class MMRNeuralQueryTransformer implements MMRQueryTransformer<NeuralQueryBuilder> {

    @Override
    public void transform(NeuralQueryBuilder queryBuilder, ActionListener<Void> listener, MMRTransformContext mmrTransformContext) {
        try {
            EventStatsManager.increment(EventStatName.MMR_NEURAL_QUERY_TRANSFORMER);
            if (queryBuilder.maxDistance() == null && queryBuilder.minScore() == null) {
                queryBuilder.k(mmrTransformContext.getCandidates());
            }

            if (mmrTransformContext.isVectorFieldInfoResolved()) {
                listener.onResponse(null);
                return;
            }

            List<String> remoteIndices = mmrTransformContext.getRemoteIndices();
            if (remoteIndices.isEmpty() == false) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "[%s] in the mmr search extension should be provided for remote indices [%s].",
                        VECTOR_FIELD_PATH,
                        String.join(",", remoteIndices)
                    )
                );
            }

            MMRRerankContext mmrRerankContext = mmrTransformContext.getMmrRerankContext();
            String queryFieldName = queryBuilder.fieldName();
            if (queryFieldName == null) {
                throw new IllegalArgumentException("Failed to transform the neural query for MMR. Query field name should not be null.");
            }
            List<MMRVectorFieldInfo> vectorFieldInfos = collectVectorFieldInfos(
                queryFieldName,
                mmrTransformContext.getLocalIndexMetadataList()
            );

            Map<String, String> indexToFieldPathMap = new HashMap<>();
            Set<String> uniqueFieldPaths = new HashSet<>();
            for (MMRVectorFieldInfo info : vectorFieldInfos) {
                indexToFieldPathMap.put(info.getIndexName(), info.getFieldPath());
                uniqueFieldPaths.add(info.getFieldPath());
            }

            if (uniqueFieldPaths.size() == 1) {
                mmrRerankContext.setVectorFieldPath(uniqueFieldPaths.iterator().next());
            } else {
                mmrRerankContext.setIndexToVectorFieldPathMap(indexToFieldPathMap);
            }

            resolveKnnVectorFieldInfo(
                vectorFieldInfos,
                mmrTransformContext.getUserProvidedSpaceType(),
                mmrTransformContext.getUserProvidedVectorDataType(),
                mmrTransformContext.getClient(),
                ActionListener.wrap(vectorFieldInfo -> {
                    mmrRerankContext.setVectorDataType(vectorFieldInfo.getVectorDataType());
                    mmrRerankContext.setSpaceType(vectorFieldInfo.getSpaceType());
                    listener.onResponse(null);
                }, listener::onFailure)
            );
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    public String getQueryName() {
        return NeuralQueryBuilder.NAME;
    }

    private List<MMRVectorFieldInfo> collectVectorFieldInfos(String queryFieldPath, List<IndexMetadata> indexMetadataList) {
        List<MMRVectorFieldInfo> vectorFieldInfos = new ArrayList<>();

        for (IndexMetadata indexMetadata : indexMetadataList) {
            vectorFieldInfos.add(collectKnnVectorFieldInfo(indexMetadata, queryFieldPath));
        }

        return vectorFieldInfos;
    }

    private MMRVectorFieldInfo collectKnnVectorFieldInfo(IndexMetadata indexMetadata, String queryFieldPath) {
        final MMRVectorFieldInfo vectorFieldInfo = new MMRVectorFieldInfo();
        vectorFieldInfo.setIndexNameByIndexMetadata(indexMetadata);

        MappingMetadata mappingMetadata = indexMetadata.mapping();
        if (mappingMetadata == null) {
            vectorFieldInfo.setUnmapped(true);
            return vectorFieldInfo;
        }

        Map<String, Object> mapping = mappingMetadata.sourceAsMap();
        Map<String, Object> queryFieldConfig = getMMRFieldMappingByPath(mapping, queryFieldPath);
        if (queryFieldConfig == null) {
            vectorFieldInfo.setUnmapped(true);
            return vectorFieldInfo;
        }

        vectorFieldInfo.setUnmapped(false);
        vectorFieldInfo.setFieldPath(queryFieldPath);

        String fieldType = (String) queryFieldConfig.get(TYPE);
        vectorFieldInfo.setFieldType(fieldType);

        Map<String, Object> knnVectorFieldConfig = queryFieldConfig;

        if (SemanticFieldMapper.CONTENT_TYPE.equals(fieldType)) {
            Object chunkingConfig = queryFieldConfig.get(CHUNKING);
            if (chunkingConfig != null && Boolean.FALSE.equals(chunkingConfig) == false) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "Field [%s] is a semantic field with chunking enabled, which can produce multiple vectors per document. "
                            + "MMR reranking does not support multiple vectors per document.",
                        queryFieldPath
                    )
                );
            }
            String semanticInfoFieldPath = getSemanticInfoFieldFullPath(queryFieldConfig, queryFieldPath, queryFieldPath);
            String vectorFieldPath = semanticInfoFieldPath + PATH_SEPARATOR + EMBEDDING_FIELD_NAME;
            knnVectorFieldConfig = getMMRFieldMappingByPath(mapping, vectorFieldPath);
            if (knnVectorFieldConfig == null) {
                throw new IllegalStateException(
                    String.format(
                        Locale.ROOT,
                        "Failed to find the vector field [%s] from index mapping for the semantic field [%s] when transform the neural query for MMR.",
                        vectorFieldPath,
                        queryFieldPath
                    )
                );
            }
            String vectorFieldType = (String) knnVectorFieldConfig.get(TYPE);
            if (KNNVectorFieldMapper.CONTENT_TYPE.equals(vectorFieldType) == false) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "Field [%s] is a semantic field with a non-KNN embedding [%s]. MMR reranking only can support knn_vector field.",
                        queryFieldPath,
                        vectorFieldType
                    )
                );
            }
            // set the info for the actual vector field
            vectorFieldInfo.setFieldType(vectorFieldType);
            vectorFieldInfo.setFieldPath(vectorFieldPath);
        } else if (KNNVectorFieldMapper.CONTENT_TYPE.equals(fieldType) == false) {
            // Field is neither semantic nor KNN vector, skip further processing
            return vectorFieldInfo;
        }

        vectorFieldInfo.setKnnConfig(knnVectorFieldConfig);

        return vectorFieldInfo;
    }

}
