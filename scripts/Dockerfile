# Build with the following command
# docker build --tag="opensearch-zscore-test:2.11.0" .
FROM opensearchproject/opensearch:2.11.0
# Remove previous neural search plugin and install new one
RUN /usr/share/opensearch/bin/opensearch-plugin remove opensearch-neural-search
# Make sure the build is preset, to create it `./gradlew clean assemble -Dopensearch.version="2.11.0"`
# Then copy it locally `cp ../build/distributions/opensearch-neural-search-2.11.0.0-SNAPSHOT.zip .`
COPY opensearch-neural-search-2.11.0.0-SNAPSHOT.zip /usr/share/opensearch/
RUN /usr/share/opensearch/bin/opensearch-plugin install -b file:/usr/share/opensearch/opensearch-neural-search-2.11.0.0-SNAPSHOT.zip
