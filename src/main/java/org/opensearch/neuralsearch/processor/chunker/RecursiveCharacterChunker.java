/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.chunker;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;

import static org.opensearch.neuralsearch.processor.chunker.ChunkerParameterParser.parseListOfStringWithDefault;
import static org.opensearch.neuralsearch.processor.chunker.ChunkerParameterParser.parsePositiveIntegerWithDefault;

public class RecursiveCharacterChunker implements Chunker {

    /** The identifier for the recursive character chunking algorithm. */
    public static final String ALGORITHM_NAME = "recursive_character";

    /** The parameter field name for specifying the delimiters. */
    public static final String DELIMITERS_FIELD = "delimiters";

    /** The default list of delimiters values used when none is specified. */
    public static final List<String> DEFAULT_DELIMITERS = List.of("\n\n", "\n", " ", "");

    /** Field name for specifying the maximum number of characters per chunk. */
    public static final String CHUNK_SIZE_FIELD = "chunk_size";

    // Default values for each non-runtime parameter
    private static final int DEFAULT_CHUNK_SIZE = 384;

    // Parameter values
    private List<String> delimiters;
    private int chunkSize;

    /**
     * Constructor that initializes the recursive character chunker with the specified parameters.
     * @param parameters a map with non-runtime parameters to be parsed
     */
    public RecursiveCharacterChunker(final Map<String, Object> parameters) {
        parseParameters(parameters);
    }

    /**
     * Parse the parameters for recursive character algorithm.
     * Throw IllegalArgumentException when parameters are invalid.
     *
     * @param parameters a map with non-runtime parameters as the following:
     * 1. chunk_size: the max length for each chunked passage
     * Requirements:
     * - chunk_size must be a positive integer
     */
    @Override
    public void parseParameters(Map<String, Object> parameters) {
        this.delimiters = parseListOfStringWithDefault(parameters, DELIMITERS_FIELD, DEFAULT_DELIMITERS);
        this.chunkSize = parsePositiveIntegerWithDefault(parameters, CHUNK_SIZE_FIELD, DEFAULT_CHUNK_SIZE);
    }

    @Override
    public List<String> chunk(String content, Map<String, Object> runtimeParameters) {
        return splitText(content, delimiters, chunkSize, String::length);
    }

    private List<String> splitText(String text, List<String> delimiters, int chunkSize, Function<String, Integer> lengthFunction) {
        List<String> finalChunks = new ArrayList<>();
        String delimiter = delimiters.getLast();
        List<String> newDelimiters = new ArrayList<>();

        for (int i = 0; i < delimiters.size(); i++) {
            String sep = delimiters.get(i);
            String delimiterToUse = delimiter;

            if (sep.isEmpty()) {
                delimiter = sep;
                break;
            }

            if (Pattern.compile(Pattern.quote(delimiterToUse)).matcher(text).find()) {
                delimiter = sep;
                for (int j = i + 1; j < delimiters.size(); j++) {
                    newDelimiters.add(delimiters.get(j));
                }
                break;
            }
        }

        List<String> splits = splitByLiteralDelimiter(text, delimiter);
        List<String> goodSplits = new ArrayList<>();
        String sepToUse = delimiter;

        for (String split : splits) {
            if (lengthFunction.apply(split) < chunkSize) {
                goodSplits.add(split);
            } else {
                if (!goodSplits.isEmpty()) {
                    finalChunks.addAll(mergeSplits(goodSplits, sepToUse, chunkSize, lengthFunction));
                    goodSplits.clear();
                }

                if (newDelimiters.isEmpty()) {
                    finalChunks.add(split);
                } else {
                    finalChunks.addAll(splitText(split, newDelimiters, chunkSize, lengthFunction));
                }
            }
        }

        if (!goodSplits.isEmpty()) {
            finalChunks.addAll(mergeSplits(goodSplits, sepToUse, chunkSize, lengthFunction));
        }

        return finalChunks;
    }

    private List<String> splitByLiteralDelimiter(String text, String delimiter) {
        List<String> result = new ArrayList<>();
        if (delimiter.isEmpty()) {
            for (char c : text.toCharArray()) {
                result.add(String.valueOf(c));
            }
        } else {
            int index;
            int start = 0;
            while ((index = text.indexOf(delimiter, start)) >= 0) {
                result.add(text.substring(start, index));
                start = index + delimiter.length();
            }
            result.add(text.substring(start));
        }
        return result;
    }

    private List<String> mergeSplits(List<String> splits, String delimiter, int chunkSize, Function<String, Integer> lengthFunction) {
        List<String> result = new ArrayList<>();
        StringBuilder currentText = new StringBuilder();

        for (String split : splits) {
            String potentialText = !currentText.isEmpty() ? currentText + delimiter + split : split;

            if (lengthFunction.apply(potentialText) > chunkSize && !currentText.isEmpty()) {
                result.add(currentText.toString());
                currentText = new StringBuilder(split);
            } else {
                if (!currentText.isEmpty()) {
                    currentText.append(delimiter);
                }
                currentText.append(split);
            }
        }

        if (!currentText.isEmpty()) {
            result.add(currentText.toString());
        }

        return result;
    }
}
