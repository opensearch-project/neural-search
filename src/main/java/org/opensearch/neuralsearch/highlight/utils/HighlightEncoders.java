/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight.utils;

/**
 * Shared HTML encoding helpers used by the synchronous fetch-phase highlighter
 * and the batch response processor. Kept here so both code paths produce the
 * exact same output for the {@code encoder: html} option.
 */
public final class HighlightEncoders {

    private HighlightEncoders() {}

    /** Escape HTML-significant characters in {@code text}. */
    public static String htmlEscape(String text) {
        StringBuilder sb = new StringBuilder(text.length());
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            switch (c) {
                case '&' -> sb.append("&amp;");
                case '<' -> sb.append("&lt;");
                case '>' -> sb.append("&gt;");
                case '"' -> sb.append("&quot;");
                case '\'' -> sb.append("&#39;");
                default -> sb.append(c);
            }
        }
        return sb.toString();
    }

    /**
     * HTML-escape the text portions of an already-highlighted response while
     * preserving the highlight tags themselves.
     */
    public static String htmlEncodePreservingTags(String highlighted, String preTag, String postTag) {
        StringBuilder result = new StringBuilder(highlighted.length());
        int cursor = 0;
        while (cursor < highlighted.length()) {
            int preIdx = highlighted.indexOf(preTag, cursor);
            if (preIdx < 0) {
                result.append(htmlEscape(highlighted.substring(cursor)));
                break;
            }
            result.append(htmlEscape(highlighted.substring(cursor, preIdx)));
            result.append(preTag);
            int afterPre = preIdx + preTag.length();
            int postIdx = highlighted.indexOf(postTag, afterPre);
            if (postIdx < 0) {
                result.append(htmlEscape(highlighted.substring(afterPre)));
                break;
            }
            result.append(htmlEscape(highlighted.substring(afterPre, postIdx)));
            result.append(postTag);
            cursor = postIdx + postTag.length();
        }
        return result.toString();
    }
}
