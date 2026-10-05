/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec;

import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.opensearch.neuralsearch.sparse.common.SparseConstants;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Locale;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Naming of the per-field, per-segment engine file, shared by the writer that creates it and the
 * readers that look it up.
 */
public class CodecUtils {
    private static String buildIndexFilePrefix(String segmentName) {
        return String.format(Locale.ROOT, "%s_", segmentName);
    }

    private static String buildIndexFileSuffix(String fieldName, String extension) {
        return String.format(Locale.ROOT, "_%s%s", fieldName, extension);
    }

    /**
     * Builds a native index file name in the format: {@code <segmentName>_<version>_<fieldName><extension>}.
     *
     * @param segmentName        the Lucene segment name
     * @param latestBuildVersion the native engine version
     * @param fieldName          the sparse vector field name
     * @param extension          the engine file extension
     * @return the constructed file name
     */
    public static String buildIndexFileName(String segmentName, String latestBuildVersion, String fieldName, String extension) {
        return String.format(
            Locale.ROOT,
            "%s%s%s",
            buildIndexFilePrefix(segmentName),
            latestBuildVersion,
            buildIndexFileSuffix(fieldName, extension)
        );
    }

    /**
     * Returns engine files for a field from the segment, sorted by name length.
     * Appends {@link SparseConstants#COMPOUND_EXTENSION} to the extension when compound file is used.
     *
     * @param extension   the base engine file extension
     * @param fieldName   the sparse vector field name
     * @param segmentInfo the segment to search
     * @return matching engine file names sorted by length
     */
    public static List<String> getEngineFiles(String extension, String fieldName, SegmentInfo segmentInfo) {
        /*
         * In case of compound file, extension would be <engine-extension> + c otherwise <engine-extension>
         */
        String engineExtension = segmentInfo.getUseCompoundFile() ? extension + SparseConstants.COMPOUND_EXTENSION : extension;
        final String engineSuffix = fieldName.isEmpty() ? engineExtension : "_" + fieldName + engineExtension;
        List<String> engineFiles = segmentInfo.files()
            .stream()
            .filter(fileName -> fileName.endsWith(engineSuffix))
            .sorted(Comparator.comparingInt(String::length))
            .collect(Collectors.toList());
        return engineFiles;
    }

    /**
     * The filesystem directory behind a Lucene {@link Directory}, which nsparse needs because it
     * maps files itself rather than reading them through an
     * {@link org.apache.lucene.store.IndexInput}.
     *
     * @param directory the directory to unwrap
     * @return the underlying filesystem path
     * @throws IOException if the directory is not filesystem-backed
     */
    public static Path resolveDirectoryPath(Directory directory) throws IOException {
        Directory unwrapped = FilterDirectory.unwrap(directory);
        if (unwrapped instanceof FSDirectory fsDirectory) {
            return fsDirectory.getDirectory();
        }
        throw new IOException("Cannot resolve a filesystem path from directory type: " + directory.getClass().getName());
    }

    /**
     * The filesystem path of one file in a {@link Directory}. The file is not required to exist.
     *
     * @param directory the directory holding the file
     * @param fileName  the file's name within the directory
     * @return the file's absolute path
     * @throws IOException if the directory is not filesystem-backed
     */
    public static String resolveFilePath(Directory directory, String fileName) throws IOException {
        return resolveDirectoryPath(directory).resolve(fileName).toString();
    }
}
