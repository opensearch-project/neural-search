/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.mapper;

import lombok.Getter;
import lombok.Setter;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.mapper.BinaryFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.MapperParsingException;
import org.opensearch.index.mapper.MatchOnlyTextFieldMapper;
import org.opensearch.index.mapper.ParametrizedFieldMapper;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.index.mapper.TokenCountFieldMapper;
import org.opensearch.index.mapper.WildcardFieldMapper;
import org.opensearch.neuralsearch.constants.MappingConstants;
import org.opensearch.neuralsearch.mapper.semanticfieldtypes.SemanticFieldTypeFactory;
import org.opensearch.neuralsearch.mapper.semanticfieldtypes.SemanticParameters;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.MODEL_ID;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.RAW_FIELD_TYPE;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.SEARCH_MODEL_ID;
import static org.opensearch.neuralsearch.constants.SemanticFieldConstants.SEMANTIC_INFO_FIELD_NAME;

/**
 * FieldMapper for the semantic field. It will hold a delegate field mapper to delegate the data parsing and query work
 * based on the raw_field_type.
 */
public class SemanticFieldMapper extends ParametrizedFieldMapper {
    public static final String CONTENT_TYPE = "semantic";
    private final SemanticParameters semanticParameters;

    @Setter
    @Getter
    private ParametrizedFieldMapper delegateFieldMapper;

    protected SemanticFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo,
        ParametrizedFieldMapper delegateFieldMapper,
        SemanticParameters semanticParameters
    ) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
        this.delegateFieldMapper = delegateFieldMapper;
        this.semanticParameters = semanticParameters;
    }

    @Override
    public Builder getMergeBuilder() {
        Builder semanticFieldMapperBuilder = (Builder) new Builder(simpleName(), SemanticFieldTypeFactory.getInstance()).init(this);
        ParametrizedFieldMapper.Builder delegateBuilder = delegateFieldMapper.getMergeBuilder();
        semanticFieldMapperBuilder.setDelegateBuilder(delegateBuilder);
        return semanticFieldMapperBuilder;
    }

    @Override
    public final ParametrizedFieldMapper merge(Mapper mergeWith) {
        if (mergeWith instanceof SemanticFieldMapper) {
            try {
                delegateFieldMapper = delegateFieldMapper.merge(((SemanticFieldMapper) mergeWith).delegateFieldMapper);
            } catch (IllegalArgumentException e) {
                String err = "Failed to update the mapper ["
                    + this.name()
                    + "] because failed to update the delegate "
                    + "mapper for the raw_field_type "
                    + this.semanticParameters.getRawFieldType()
                    + ". "
                    + e.getMessage();
                throw new IllegalArgumentException(err, e);
            }
        }
        return super.merge(mergeWith);
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        delegateFieldMapper.parse(context);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    public static class Builder extends ParametrizedFieldMapper.Builder {
        @Getter
        protected final Parameter<String> modelId = Parameter.stringParam(
            MODEL_ID,
            true,
            m -> ((SemanticFieldMapper) m).semanticParameters.getModelId(),
            null
        );
        @Getter
        protected final Parameter<String> searchModelId = Parameter.stringParam(
            SEARCH_MODEL_ID,
            true,
            m -> ((SemanticFieldMapper) m).semanticParameters.getSearchModelId(),
            null
        );
        @Getter
        protected final Parameter<String> rawFieldType = Parameter.stringParam(
            RAW_FIELD_TYPE,
            false,
            m -> ((SemanticFieldMapper) m).semanticParameters.getRawFieldType(),
            TextFieldMapper.CONTENT_TYPE
        );
        @Getter
        protected final Parameter<String> semanticInfoFieldName = Parameter.stringParam(
            SEMANTIC_INFO_FIELD_NAME,
            false,
            m -> ((SemanticFieldMapper) m).semanticParameters.getSemanticInfoFieldName(),
            null
        );

        @Setter
        protected ParametrizedFieldMapper.Builder delegateBuilder;
        private final SemanticFieldTypeFactory semanticFieldTypeFactory;

        protected Builder(String name, SemanticFieldTypeFactory semanticFieldTypeFactory) {
            super(name);
            this.semanticFieldTypeFactory = semanticFieldTypeFactory;
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return List.of(modelId, searchModelId, rawFieldType, semanticInfoFieldName);
        }

        @Override
        public SemanticFieldMapper build(BuilderContext context) {
            final ParametrizedFieldMapper delegateMapper = delegateBuilder.build(context);

            final SemanticParameters semanticParameters = this.getSemanticParameters();
            final MappedFieldType semanticFieldType = semanticFieldTypeFactory.createSemanticFieldType(
                delegateMapper,
                rawFieldType.getValue(),
                semanticParameters
            );

            return new SemanticFieldMapper(
                name,
                semanticFieldType,
                multiFieldsBuilder.build(this, context),
                copyTo.build(),
                delegateMapper,
                semanticParameters
            );
        }

        public SemanticParameters getSemanticParameters() {
            return new SemanticParameters(
                modelId.getValue(),
                searchModelId.getValue(),
                rawFieldType.getValue(),
                semanticInfoFieldName.getValue()
            );
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        private final static Set<String> SUPPORTED_RAW_FIELD_TYPE = Set.of(
            TextFieldMapper.CONTENT_TYPE,
            KeywordFieldMapper.CONTENT_TYPE,
            MatchOnlyTextFieldMapper.CONTENT_TYPE,
            WildcardFieldMapper.CONTENT_TYPE,
            TokenCountFieldMapper.CONTENT_TYPE,
            BinaryFieldMapper.CONTENT_TYPE
        );

        @Override
        public Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            final String rawFieldType = (String) node.getOrDefault(RAW_FIELD_TYPE, TextFieldMapper.CONTENT_TYPE);

            validateRawFieldType(rawFieldType);

            final ParametrizedFieldMapper.TypeParser typeParser = (ParametrizedFieldMapper.TypeParser) parserContext.typeParser(
                rawFieldType
            );
            final Builder semanticFieldMapperBuilder = new Builder(name, SemanticFieldTypeFactory.getInstance());

            // semantic field mapper builder parse semantic fields
            Map<String, Object> semanticConfig = extractSemanticConfig(node, semanticFieldMapperBuilder.getParameters(), rawFieldType);
            semanticFieldMapperBuilder.parse(name, parserContext, semanticConfig);

            // delegate field mapper builder parse remaining fields
            ParametrizedFieldMapper.Builder delegateBuilder = typeParser.parse(name, node, parserContext);
            semanticFieldMapperBuilder.setDelegateBuilder(delegateBuilder);

            return semanticFieldMapperBuilder;
        }

        private void validateRawFieldType(final String rawFieldType) {
            if (rawFieldType == null || !SUPPORTED_RAW_FIELD_TYPE.contains(rawFieldType)) {
                throw new IllegalArgumentException(
                    RAW_FIELD_TYPE
                        + ": ["
                        + rawFieldType
                        + "] is not supported. It "
                        + "should be one of ["
                        + String.join(", ", SUPPORTED_RAW_FIELD_TYPE)
                        + "]"
                );
            }
        }

        /**
         * In this function we will extract all the parameters defined in the semantic field mapper builder and parse it
         * later. The remaining parameters will be processed by the type parser of the raw field type. Here we cannot
         * pass the parameters defined by semantic field to the delegate type parser of the raw field type because it
         * cannot recognize them.
         * @param node field config
         * @param parameters parameters for semantic field
         * @param rawFieldType field type of the raw data
         * @return semantic field config
         */
        private Map<String, Object> extractSemanticConfig(Map<String, Object> node, List<Parameter<?>> parameters, String rawFieldType) {
            final Map<String, Object> semanticConfig = new HashMap<>();
            for (Parameter<?> parameter : parameters) {
                Object config = node.get(parameter.name);
                if (config != null) {
                    semanticConfig.put(parameter.name, config);
                    node.remove(parameter.name);
                }
            }
            semanticConfig.put(MappingConstants.TYPE, SemanticFieldMapper.CONTENT_TYPE);
            node.put(MappingConstants.TYPE, rawFieldType);
            return semanticConfig;
        }
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        builder.field(MappingConstants.TYPE, contentType());

        // semantic parameters
        final List<Parameter<?>> parameters = getMergeBuilder().getParameters();
        for (Parameter<?> parameter : parameters) {
            // By default, we will not return the default value. But raw_field_type is useful info to let users know how
            // we will handle the raw data. So we explicitly return it even it is using the default value.
            if (RAW_FIELD_TYPE.equals(parameter.name)) {
                parameter.toXContent(builder, true);
            } else {
                parameter.toXContent(builder, includeDefaults);
            }
        }

        // non-semantic parameters
        // semantic field mapper itself does not handle multi fields or copy to. The delegate field mapper will handle it.
        delegateFieldMapper.multiFields().toXContent(builder, params);
        delegateFieldMapper.copyTo().toXContent(builder, params);
        delegateFieldMapper.getMergeBuilder().toXContent(builder, includeDefaults);
    }
}
