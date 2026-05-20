/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.highlight.utils;

import org.opensearch.test.OpenSearchTestCase;

public class HighlightEncodersTests extends OpenSearchTestCase {

    // ============= htmlEscape =============

    public void testHtmlEscapeEmptyString() {
        assertEquals("", HighlightEncoders.htmlEscape(""));
    }

    public void testHtmlEscapePlainText() {
        assertEquals("hello world", HighlightEncoders.htmlEscape("hello world"));
    }

    public void testHtmlEscapeAmpersand() {
        assertEquals("a &amp; b", HighlightEncoders.htmlEscape("a & b"));
    }

    public void testHtmlEscapeLessThan() {
        assertEquals("a &lt; b", HighlightEncoders.htmlEscape("a < b"));
    }

    public void testHtmlEscapeGreaterThan() {
        assertEquals("a &gt; b", HighlightEncoders.htmlEscape("a > b"));
    }

    public void testHtmlEscapeDoubleQuote() {
        assertEquals("a &quot;b&quot; c", HighlightEncoders.htmlEscape("a \"b\" c"));
    }

    public void testHtmlEscapeSingleQuote() {
        assertEquals("a &#39;b&#39; c", HighlightEncoders.htmlEscape("a 'b' c"));
    }

    public void testHtmlEscapeAllSpecialCharacters() {
        assertEquals("&amp;&lt;&gt;&quot;&#39;", HighlightEncoders.htmlEscape("&<>\"'"));
    }

    public void testHtmlEscapeMixedContent() {
        assertEquals(
            "&lt;script&gt;alert(&quot;xss&quot;)&lt;/script&gt;",
            HighlightEncoders.htmlEscape("<script>alert(\"xss\")</script>")
        );
    }

    public void testHtmlEscapeUnicodeUnchanged() {
        assertEquals("héllo wörld 你好", HighlightEncoders.htmlEscape("héllo wörld 你好"));
    }

    // ============= htmlEncodePreservingTags =============

    public void testHtmlEncodePreservingTagsEmpty() {
        assertEquals("", HighlightEncoders.htmlEncodePreservingTags("", "<em>", "</em>"));
    }

    public void testHtmlEncodePreservingTagsNoTagsPresent() {
        // No <em>/</em> in input — entire string is escaped
        assertEquals("a &amp; b", HighlightEncoders.htmlEncodePreservingTags("a & b", "<em>", "</em>"));
    }

    public void testHtmlEncodePreservingTagsBasic() {
        // Tags preserved, surrounding text escaped
        String input = "a & <em>b & c</em> d & e";
        String expected = "a &amp; <em>b &amp; c</em> d &amp; e";
        assertEquals(expected, HighlightEncoders.htmlEncodePreservingTags(input, "<em>", "</em>"));
    }

    public void testHtmlEncodePreservingTagsCustomTags() {
        String input = "x & <mark>y & z</mark> w";
        String expected = "x &amp; <mark>y &amp; z</mark> w";
        assertEquals(expected, HighlightEncoders.htmlEncodePreservingTags(input, "<mark>", "</mark>"));
    }

    public void testHtmlEncodePreservingTagsMultipleTagPairs() {
        String input = "a < <em>b</em> c & <em>d</em> e > f";
        String expected = "a &lt; <em>b</em> c &amp; <em>d</em> e &gt; f";
        assertEquals(expected, HighlightEncoders.htmlEncodePreservingTags(input, "<em>", "</em>"));
    }

    public void testHtmlEncodePreservingTagsUnclosedPreTag() {
        // preTag with no postTag — everything after preTag is escaped, fall through "postIdx < 0" branch
        String input = "a <em>b & c";
        String result = HighlightEncoders.htmlEncodePreservingTags(input, "<em>", "</em>");
        assertEquals("a <em>b &amp; c", result);
    }

    public void testHtmlEncodePreservingTagsTagInsideTextEscaped() {
        // No real preTag at all — first branch ("preIdx < 0") runs over the entire string
        String input = "a < b > c & d";
        assertEquals("a &lt; b &gt; c &amp; d", HighlightEncoders.htmlEncodePreservingTags(input, "<em>", "</em>"));
    }

    public void testHtmlEncodePreservingTagsImmediatelyOpensWithTag() {
        // preTag at the very start — first escape segment is empty
        String input = "<em>x</em> & y";
        assertEquals("<em>x</em> &amp; y", HighlightEncoders.htmlEncodePreservingTags(input, "<em>", "</em>"));
    }

    public void testPrivateConstructor() throws Exception {
        java.lang.reflect.Constructor<HighlightEncoders> c = HighlightEncoders.class.getDeclaredConstructor();
        c.setAccessible(true);
        assertNotNull(c.newInstance());
    }
}
