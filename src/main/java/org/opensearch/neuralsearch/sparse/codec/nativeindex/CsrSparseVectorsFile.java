/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.sparse.codec.nativeindex;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.neuralsearch.sparse.codec.CodecUtils;
import org.opensearch.neuralsearch.sparse.quantization.ByteQuantizer;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * Stages a segment's sparse vectors as the pair of files
 * {@code IDMapIndex::read_csr_and_ids} reads, so nsparse maps them instead of receiving the vectors
 * over JNI.
 *
 * The CSR file is nsparse's native layout ({@code nsparse/utils/csr_layout.h}) -- in-memory widths,
 * little-endian, so the mapped reader can reinterpret the arrays in place rather than copy them:
 *
 * <pre>
 *   int64  header[3]        rows, cols, nnz
 *   int32  indptr[rows + 1] idx_t
 *   uint16 indices[nnz]     term_t
 *          &lt;padding to 4&gt;   present iff nnz is odd
 *   value  values[nnz]      uint8 codes, or float32
 * </pre>
 *
 * The value width is the one the target index borrows at, its {@code code_element_size()}: one byte
 * for a quantizing layout, which searches over codes, and four for an unquantized one, which searches
 * over floats. nsparse sizes the file by that width, so the wrong one is rejected at open rather than
 * misread -- which is why the choice is a factory method rather than a flag. Use
 * {@link #forQuantizedIndex} or {@link #forUnquantizedIndex} to match the index
 * {@link NativeIndexParameters} will build.
 *
 * The padding is to {@code alignof(float)} either way, since that is what
 * {@code csr_layout::native_values_offset} pads to whatever the width is -- so an odd {@code nnz} of
 * 2-byte terms is followed by two zero bytes even when the values that follow need no alignment.
 *
 * The id file carries what a CSR file has no room for, the doc id of each row:
 *
 * <pre>
 *   int64 count
 *   int32 external_ids[count]   idx_t, row-aligned with the CSR
 * </pre>
 *
 * nsparse checks both files against these exact sizes and cross-checks {@code count} against the CSR
 * row count, so a layout disagreement fails at open rather than mis-mapping doc ids during search. In
 * particular a codes CSR handed to an unquantized index fails the size check rather than being read as
 * floats.
 *
 * A CSR file cannot be written in one forward pass: {@code nnz} and {@code cols} are only known after
 * the last document, and the indices and the values are separate sections while the doc values hand
 * them over interleaved. So the two nnz-sized arrays go to their own scratch files as they stream,
 * {@code indptr} stays on the heap (4 bytes per row), and {@link #finish} assembles them. Peak heap
 * is one vector plus indptr; the cost is writing the nnz arrays twice.
 */
class CsrSparseVectorsFile implements Closeable {

    /** {@code int64 rows, cols, nnz}. */
    static final int HEADER_BYTES = 3 * Long.BYTES;

    /** {@code int64 count}, ahead of the ids. */
    static final int ID_HEADER_BYTES = Long.BYTES;

    /** nsparse stores token ids as {@code term_t}, a {@code uint16}. */
    static final int MAX_TOKEN_ID = 0xFFFF;

    /** What a quantizing index reports as its {@code code_element_size()}. */
    static final int QUANTIZED_VALUE_BYTES = Byte.BYTES;

    /** What an unquantized index borrows at -- {@code MmapIndex}'s default {@code code_element_size()}. */
    static final int FLOAT_VALUE_BYTES = Float.BYTES;

    private final Directory directory;
    private final IOContext context;
    private final String segmentName;

    /** Non-null iff the values are staged as 8-bit codes; null stages them as float32. */
    private final ByteQuantizer quantizer;

    private IndexOutput indicesScratch;
    private IndexOutput valuesScratch;

    /** Prefix sums of the per-row nnz, so {@code indptr[0] == 0} and {@code indptr[rows] == nnz}. */
    private int[] indptr = new int[1024];
    private int rows;
    private long nnz;

    /** The assembled files, non-null once {@link #finish} has succeeded. */
    private String csrFileName;
    private String idFileName;

    /**
     * Opens the scratch files the vectors stream into.
     *
     * @param directory   the segment directory, which must be filesystem-backed because nsparse maps
     *                    the result itself
     * @param context     the IO context the segment is being written with
     * @param segmentName prefix for every file this creates
     * @param quantizer   the ingest quantizer for a codes file, or null to stage float32
     * @throws IOException if the directory is not filesystem-backed, or a scratch file cannot be
     *                     created
     */
    private CsrSparseVectorsFile(Directory directory, IOContext context, String segmentName, ByteQuantizer quantizer) throws IOException {
        this.directory = directory;
        this.context = context;
        this.segmentName = segmentName;
        this.quantizer = quantizer;
        // Fails before any vector is written rather than after the whole segment has been staged.
        CodecUtils.resolveDirectoryPath(directory);
        boolean success = false;
        try {
            this.indicesScratch = directory.createTempOutput(segmentName, "csr_indices", context);
            this.valuesScratch = directory.createTempOutput(segmentName, "csr_values", context);
            success = true;
        } finally {
            if (success == false) {
                close();
            }
        }
    }

    /**
     * Stages values as 8-bit codes, for the {@code seismic_sq} and {@code disk_seismic_sq} layouts.
     *
     * @param quantizer the ingest quantizer, whose ceiling must be the {@code vmax} the index is built
     *                  with -- nothing re-encodes the codes, so the two have to agree or the scores
     *                  decode to a different scale
     */
    static CsrSparseVectorsFile forQuantizedIndex(Directory directory, IOContext context, String segmentName, ByteQuantizer quantizer)
        throws IOException {
        return new CsrSparseVectorsFile(directory, context, segmentName, java.util.Objects.requireNonNull(quantizer));
    }

    /**
     * Stages values as float32, for the unquantized layouts -- {@code inverted}, and the plain seismic
     * ones. They search over floats, so quantizing here would change their scores, not just their size.
     */
    static CsrSparseVectorsFile forUnquantizedIndex(Directory directory, IOContext context, String segmentName) throws IOException {
        return new CsrSparseVectorsFile(directory, context, segmentName, null);
    }

    /** Bytes per staged value, which has to equal the target index's {@code code_element_size()}. */
    int valueBytes() {
        return quantizer == null ? FLOAT_VALUE_BYTES : QUANTIZED_VALUE_BYTES;
    }

    /**
     * Appends one document's vector as the next CSR row. Tokens keep the order the doc values stored
     * them in; nsparse does not require them sorted.
     *
     * @param tokens  the token ids, each of which must fit {@code term_t}
     * @param weights the weights, index-aligned with {@code tokens}
     */
    void addVector(List<Integer> tokens, List<Float> weights) throws IOException {
        for (int i = 0; i < tokens.size(); i++) {
            int token = tokens.get(i);
            // The mapped reader deliberately skips this check -- scanning the indices array at open
            // would fault the whole thing in -- and the JNI insert path that does check
            // (nsparse_wrapper.cpp) is not on this route. Narrowing silently would alias distinct
            // tokens onto one term and score them together, so it has to be caught here.
            if (token < 0 || token > MAX_TOKEN_ID) {
                throw new IllegalArgumentException(
                    "sparse token id " + token + " is outside the range [0, " + MAX_TOKEN_ID + "] supported by the native engine"
                );
            }
            indicesScratch.writeShort((short) token);
            if (quantizer == null) {
                // Raw bits rather than floatToIntBits, which canonicalizes a NaN weight into a
                // different one than the streaming path would hand the index.
                valuesScratch.writeInt(Float.floatToRawIntBits(weights.get(i)));
            } else {
                // The same ByteQuantizer the streaming path's index quantizes with, so a weight lands
                // on the same code either way and the two writers produce identical indexes.
                valuesScratch.writeByte(quantizer.quantize(weights.get(i)));
            }
        }
        nnz += tokens.size();
        // idx_t is int32, so the running offset has to fit one. Checked per row rather than only in
        // finish() so the failure names a bound the caller can act on before the file is assembled.
        if (nnz > Integer.MAX_VALUE) {
            throw new IllegalStateException(
                "sparse segment has " + nnz + " non-zeros, more than the native engine's 32-bit CSR offsets hold"
            );
        }
        indptr = ArrayUtil.grow(indptr, rows + 2);
        indptr[++rows] = (int) nnz;
    }

    /**
     * Assembles the scratch files into the CSR file and writes the id file. No vector may be added
     * afterwards.
     *
     * @param dimension the field's dimension, which nsparse checks its own against
     * @param docIds    the doc id of each row, in the order the rows were added
     */
    void finish(int dimension, int[] docIds) throws IOException {
        if (csrFileName != null) {
            throw new IllegalStateException("CSR file has already been assembled: " + csrFileName);
        }
        if (rows == 0) {
            throw new IllegalStateException("CSR file needs at least one row; nsparse rejects an empty one");
        }
        if (docIds.length != rows) {
            // nsparse would catch this too, but only after mapping the CSR, and its message names
            // rows rather than the caller's mistake.
            throw new IllegalStateException("got " + docIds.length + " doc ids for " + rows + " CSR rows");
        }
        writeCsrFile(dimension);
        writeIdFile(docIds);
    }

    private void writeCsrFile(int dimension) throws IOException {
        // Closed before being read back: the writes are buffered, and nothing guarantees the tail of
        // either array has reached the directory until then.
        IOUtils.close(indicesScratch, valuesScratch);
        String indicesName = indicesScratch.getName();
        String valuesName = valuesScratch.getName();
        indicesScratch = null;
        valuesScratch = null;

        IndexOutput csrOutput = directory.createTempOutput(segmentName, "csr", context);
        String csrName = csrOutput.getName();
        boolean success = false;
        try {
            csrOutput.writeLong(rows);
            csrOutput.writeLong(dimension);
            csrOutput.writeLong(nnz);
            for (int row = 0; row <= rows; row++) {
                csrOutput.writeInt(indptr[row]);
            }
            copyScratch(csrOutput, indicesName);
            // The mapped reader reinterprets the values in place, so they have to start on a 4-byte
            // boundary. The header and indptr are multiples of 4; an odd nnz of 2-byte terms is not.
            for (int i = 0; i < paddingBytes(nnz); i++) {
                csrOutput.writeByte((byte) 0);
            }
            copyScratch(csrOutput, valuesName);
            IOUtils.close(csrOutput);
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(csrOutput);
                deleteFilesIgnoringExceptions(directory, csrName);
            }
            // Deleted through the directory, not the filesystem: on flush and merge alike this is a
            // TrackingDirectoryWrapper whose created-file set becomes segmentInfo.files(), and only
            // its own deleteFile takes a name back out of that set.
            deleteFilesIgnoringExceptions(directory, indicesName, valuesName);
        }
        csrFileName = csrName;
    }

    private void writeIdFile(int[] docIds) throws IOException {
        IndexOutput idOutput = directory.createTempOutput(segmentName, "csr_ids", context);
        String idName = idOutput.getName();
        boolean success = false;
        try {
            idOutput.writeLong(docIds.length);
            for (int docId : docIds) {
                idOutput.writeInt(docId);
            }
            IOUtils.close(idOutput);
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(idOutput);
                deleteFilesIgnoringExceptions(directory, idName);
            }
        }
        idFileName = idName;
    }

    /**
     * Bytes of padding before the values, given the {@code nnz} 2-byte terms that precede them.
     *
     * To {@code alignof(float)}, which is what {@code csr_layout::native_values_offset} pads to
     * whatever the value width is -- 4 satisfies any narrower code width, so the codes do not get to
     * skip it.
     */
    static int paddingBytes(long nnz) {
        return (int) ((Float.BYTES - (nnz * Short.BYTES) % Float.BYTES) % Float.BYTES);
    }

    /** The filesystem path of the CSR file, valid once {@link #finish} has run. */
    String resolveCsrPath() throws IOException {
        return CodecUtils.resolveFilePath(directory, requireFinished(csrFileName));
    }

    /** The filesystem path of the id file, valid once {@link #finish} has run. */
    String resolveIdPath() throws IOException {
        return CodecUtils.resolveFilePath(directory, requireFinished(idFileName));
    }

    private static String requireFinished(String fileName) {
        if (fileName == null) {
            throw new IllegalStateException("CSR files have not been assembled yet");
        }
        return fileName;
    }

    private void copyScratch(IndexOutput output, String scratchName) throws IOException {
        try (IndexInput input = directory.openInput(scratchName, IOContext.READONCE)) {
            output.copyBytes(input, input.length());
        }
    }

    /**
     * Releases every file this staging area created, including the assembled ones.
     *
     * The caller must not close until nsparse is done with them: under mmap residency the index
     * borrows the vectors from the CSR file rather than copying them, so it has to stay mapped
     * through serialization of the engine file.
     */
    @Override
    public void close() {
        IOUtils.closeWhileHandlingException(indicesScratch, valuesScratch);
        if (indicesScratch != null) {
            deleteFilesIgnoringExceptions(directory, indicesScratch.getName());
            indicesScratch = null;
        }
        if (valuesScratch != null) {
            deleteFilesIgnoringExceptions(directory, valuesScratch.getName());
            valuesScratch = null;
        }
        if (csrFileName != null) {
            deleteFilesIgnoringExceptions(directory, csrFileName);
            csrFileName = null;
        }
        if (idFileName != null) {
            deleteFilesIgnoringExceptions(directory, idFileName);
            idFileName = null;
        }
    }

    /**
     * Deletes each name through the directory, ignoring failures, for the cleanup paths that are
     * already unwinding an earlier error.
     *
     * Local because {@link IOUtils} only deletes by {@link java.nio.file.Path}, and these files have
     * to go back through the directory that created them -- see {@link #writeCsrFile}.
     */
    private static void deleteFilesIgnoringExceptions(Directory directory, String... names) {
        for (String name : names) {
            try {
                directory.deleteFile(name);
            } catch (Exception ignored) {}
        }
    }
}
