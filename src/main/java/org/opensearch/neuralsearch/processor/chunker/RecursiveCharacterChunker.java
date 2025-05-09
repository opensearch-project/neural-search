/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.chunker;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.opensearch.neuralsearch.processor.chunker.ChunkerParameterParser.parseInteger;
import static org.opensearch.neuralsearch.processor.chunker.ChunkerParameterParser.parseListOfStringWithDefault;
import static org.opensearch.neuralsearch.processor.chunker.ChunkerParameterParser.parsePositiveIntegerWithDefault;

public class RecursiveCharacterChunker implements Chunker {

    public static final String ALGORITHM_NAME = "recursive_character";
    public static final String DELIMITERS_FIELD = "delimiters";
    public static final List<String> DEFAULT_DELIMITERS = List.of("\n\n", "\n", " ", "");
    public static final String CHUNK_SIZE_FIELD = "chunk_size";
    public static final String MAX_CHUNK_LIMIT_FIELD = "max_chunk_limit";
    private static final int DEFAULT_CHUNK_SIZE = 384;

    private List<String> delimiters;
    private int chunkSize;

    public RecursiveCharacterChunker(final Map<String, Object> parameters) {
        parseParameters(parameters);
    }

    @Override
    public void parseParameters(Map<String, Object> parameters) {
        this.delimiters = parseListOfStringWithDefault(parameters, DELIMITERS_FIELD, DEFAULT_DELIMITERS);
        this.chunkSize = parsePositiveIntegerWithDefault(parameters, CHUNK_SIZE_FIELD, DEFAULT_CHUNK_SIZE);
    }

    @Override
    public List<String> chunk(String content, Map<String, Object> runtimeParameters) {
        int runtimeMaxChunkLimit = parseInteger(runtimeParameters, MAX_CHUNK_LIMIT_FIELD);
        List<String> processedChunks = splitRecursive(content, delimiters, chunkSize, 0);

        if (runtimeMaxChunkLimit > 0 && processedChunks.size() > runtimeMaxChunkLimit) {
            List<String> limitedChunks = new ArrayList<>();
            if (runtimeMaxChunkLimit > 1) {
                limitedChunks.addAll(processedChunks.subList(0, runtimeMaxChunkLimit - 1));
            }

            StringBuilder mergedTailBuilder = new StringBuilder();
            int mergeStartIndex = Math.max(0, runtimeMaxChunkLimit - 1);

            for (int i = mergeStartIndex; i < processedChunks.size(); i++) {
                String chunk = processedChunks.get(i);
                if (!mergedTailBuilder.isEmpty() && chunk != null && !chunk.isEmpty()) {
                    char last = mergedTailBuilder.charAt(mergedTailBuilder.length() - 1);
                    char first = chunk.charAt(0);
                    if (!Character.isWhitespace(last) && !Character.isWhitespace(first)) {
                        mergedTailBuilder.append(" ");
                    }
                }
                if (chunk != null) {
                    mergedTailBuilder.append(chunk);
                }
            }
            limitedChunks.add(mergedTailBuilder.toString());
            return limitedChunks;
        } else {
            return processedChunks;
        }
    }

    private List<String> splitRecursive(String text, List<String> delimiters, int chunkSize, int level) {
        if (text == null || text.isEmpty()) {
            return new ArrayList<>();
        }
        if (level >= delimiters.size()) {
            return mergeSplits(List.of(text.split("(?<=.)")), "", chunkSize);
        }

        String delimiter = delimiters.get(level);
        List<String> splits = splitByLiteralDelimiter(text, delimiter);
        List<String> chunks = mergeSplits(splits, delimiter, chunkSize);

        boolean tooLong = chunks.stream().anyMatch(s -> s != null && s.length() > chunkSize);
        if (tooLong) {
            List<String> refined = new ArrayList<>();
            for (String chunk : chunks) {
                if (chunk != null && chunk.length() > chunkSize) {
                    refined.addAll(splitRecursive(chunk, delimiters, chunkSize, level + 1));
                } else if (chunk != null) {
                    refined.add(chunk);
                }
            }
            return refined;
        } else {
            return chunks.stream().filter(Objects::nonNull).toList();
        }
    }

    private List<String> splitByLiteralDelimiter(String text, String delimiter) {
        List<String> result = new ArrayList<>();
        if (delimiter.isEmpty()) {
            for (char c : text.toCharArray()) {
                result.add(String.valueOf(c));
            }
        } else {
            int start = 0;
            int next = text.indexOf(delimiter, start);
            while (next != -1) {
                String part = text.substring(start, next);
                if (!part.isEmpty()) {
                    result.add(part);
                }
                result.add(delimiter);
                start = next + delimiter.length();
                next = text.indexOf(delimiter, start);
            }
            if (start < text.length()) {
                result.add(text.substring(start));
            }
        }
        return result;
    }

    private List<String> mergeSplits(List<String> splits, String currentDelimiter, int chunkSize) {
        List<String> merged = new ArrayList<>();
        StringBuilder builder = new StringBuilder();

        for (String piece : splits) {
            if (piece == null) continue;
            boolean isDelimiter = !currentDelimiter.isEmpty() && piece.equals(currentDelimiter);

            if (builder.isEmpty() && isDelimiter) {
                continue;
            }

            if (builder.length() + piece.length() <= chunkSize) {
                builder.append(piece);
            } else {
                if (!builder.isEmpty()) {
                    String chunk = builder.toString().strip();
                    if (!chunk.isEmpty()) {
                        merged.add(chunk);
                    }
                }
                builder = isDelimiter ? new StringBuilder() : new StringBuilder(piece);
            }
        }

        if (!builder.isEmpty()) {
            String chunk = builder.toString().strip();
            if (!chunk.isEmpty()) {
                merged.add(chunk);
            }
        }
        return merged;
    }
}
