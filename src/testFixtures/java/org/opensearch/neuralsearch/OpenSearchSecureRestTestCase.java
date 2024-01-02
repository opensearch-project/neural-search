/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.ParseException;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.message.BasicHeader;
import org.apache.http.ssl.SSLContextBuilder;
import static org.opensearch.knn.common.KNNConstants.MODEL_INDEX_NAME;
import static org.opensearch.neuralsearch.TestUtils.NEURAL_SEARCH_BWC_PREFIX;
import static org.opensearch.neuralsearch.TestUtils.OPENDISTRO_SECURITY;
import static org.opensearch.neuralsearch.TestUtils.OPENSEARCH_SYSTEM_INDEX_PREFIX;
import static org.opensearch.neuralsearch.TestUtils.SECURITY_AUDITLOG_PREFIX;
import static org.opensearch.neuralsearch.TestUtils.SKIP_DELETE_MODEL_INDEX;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.After;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.rest.OpenSearchRestTestCase;

/**
 * Base class for running the integration tests on a secure cluster. The plugin IT test should either extend this
 * class or create another base class by extending this class to make sure that their IT can be run on secure clusters.
 */
public abstract class OpenSearchSecureRestTestCase extends OpenSearchRestTestCase {

    private static final String PROTOCOL_HTTP = "http";
    private static final String PROTOCOL_HTTPS = "https";
    private static final String SYS_PROPERTY_KEY_HTTPS = "https";
    private static final String SYS_PROPERTY_KEY_CLUSTER_ENDPOINT = "tests.rest.cluster";
    private static final String SYS_PROPERTY_KEY_USER = "user";
    private static final String SYS_PROPERTY_KEY_PASSWORD = "password";
    private static final String DEFAULT_SOCKET_TIMEOUT = "60s";
    private static final String INTERNAL_INDICES_PREFIX = ".";
    private static String protocol;

    private final Set<String> IMMUTABLE_INDEX_PREFIXES = Set.of(
        NEURAL_SEARCH_BWC_PREFIX,
        SECURITY_AUDITLOG_PREFIX,
        OPENSEARCH_SYSTEM_INDEX_PREFIX
    );

    @Override
    protected String getProtocol() {
        if (protocol == null) {
            protocol = readProtocolFromSystemProperty();
        }
        return protocol;
    }

    private String readProtocolFromSystemProperty() {
        final boolean isHttps = Optional.ofNullable(System.getProperty(SYS_PROPERTY_KEY_HTTPS)).map("true"::equalsIgnoreCase).orElse(false);
        if (!isHttps) {
            return PROTOCOL_HTTP;
        }

        // currently only external cluster is supported for security enabled testing
        if (Optional.ofNullable(System.getProperty(SYS_PROPERTY_KEY_CLUSTER_ENDPOINT)).isEmpty()) {
            throw new RuntimeException("cluster url should be provided for security enabled testing");
        }
        return PROTOCOL_HTTPS;
    }

    @Override
    protected RestClient buildClient(Settings settings, HttpHost[] hosts) throws IOException {
        final RestClientBuilder builder = RestClient.builder(hosts);
        if (PROTOCOL_HTTPS.equals(getProtocol())) {
            configureHttpsClient(builder, settings);
        } else {
            configureClient(builder, settings);
        }

        return builder.build();
    }

    private void configureHttpsClient(final RestClientBuilder builder, final Settings settings) {
        final Map<String, String> headers = ThreadContext.buildDefaultHeaders(settings);
        final Header[] defaultHeaders = new Header[headers.size()];
        int i = 0;
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            defaultHeaders[i++] = new BasicHeader(entry.getKey(), entry.getValue());
        }
        builder.setDefaultHeaders(defaultHeaders);
        builder.setHttpClientConfigCallback(httpClientBuilder -> {
            final String userName = Optional.ofNullable(System.getProperty(SYS_PROPERTY_KEY_USER))
                .orElseThrow(() -> new RuntimeException("user name is missing"));
            final String password = Optional.ofNullable(System.getProperty(SYS_PROPERTY_KEY_PASSWORD))
                .orElseThrow(() -> new RuntimeException("password is missing"));
            final BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            final AuthScope anyScope = new AuthScope(null, -1);
            credentialsProvider.setCredentials(anyScope, new UsernamePasswordCredentials(userName, password));
            try {
                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
                    // disable the certificate since our testing cluster just uses the default security configuration
                    .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                    .setSSLContext(SSLContextBuilder.create().loadTrustMaterial(null, (chains, authType) -> true).build());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        final String socketTimeoutString = settings.get(CLIENT_SOCKET_TIMEOUT);
        final TimeValue socketTimeout = TimeValue.parseTimeValue(
            socketTimeoutString == null ? DEFAULT_SOCKET_TIMEOUT : socketTimeoutString,
            CLIENT_SOCKET_TIMEOUT
        );
        builder.setRequestConfigCallback(conf -> conf.setSocketTimeout(Math.toIntExact(socketTimeout.getMillis())));
        if (settings.hasValue(CLIENT_PATH_PREFIX)) {
            builder.setPathPrefix(settings.get(CLIENT_PATH_PREFIX));
        }
    }

    /**
     * wipeAllIndices won't work since it cannot delete security index. Use deleteExternalIndices instead.
     */
    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @After
    public void deleteExternalIndices() throws IOException, ParseException {
        final Response response = client().performRequest(new Request("GET", "/_cat/indices?format=json" + "&expand_wildcards=all"));
        try (
            final XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                response.getEntity().getContent()
            )
        ) {
            final XContentParser.Token token = parser.nextToken();
            final List<Map<String, Object>> parserList;
            if (token == XContentParser.Token.START_ARRAY) {
                parserList = parser.listOrderedMap().stream().map(obj -> (Map<String, Object>) obj).collect(Collectors.toList());
            } else {
                parserList = Collections.singletonList(parser.mapOrdered());
            }

            final List<String> externalIndices = parserList.stream()
                .map(index -> (String) index.get("index"))
                .filter(indexName -> indexName != null)
                .filter(indexName -> !indexName.startsWith(INTERNAL_INDICES_PREFIX))
                .collect(Collectors.toList());

            for (final String indexName : externalIndices) {
                if (!skipDeleteIndex(indexName)) {
                    adminClient().performRequest(new Request("DELETE", "/" + indexName));
                }
            }
        }
    }

    private boolean getSkipDeleteModelIndexFlag() {
        return Boolean.parseBoolean(System.getProperty(SKIP_DELETE_MODEL_INDEX, "false"));
    }

    private boolean skipDeleteModelIndex(String indexName) {
        return (MODEL_INDEX_NAME.equals(indexName) && getSkipDeleteModelIndexFlag());
    }

    private boolean skipDeleteIndex(String indexName) {
        if (indexName != null
            && !OPENDISTRO_SECURITY.equals(indexName)
            && IMMUTABLE_INDEX_PREFIXES.stream().noneMatch(indexName::startsWith)
            && !skipDeleteModelIndex(indexName)) {
            return false;
        }

        return true;
    }
}
