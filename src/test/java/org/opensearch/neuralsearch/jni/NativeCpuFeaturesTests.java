/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.jni;

import java.util.Set;
import java.util.stream.Stream;

import org.opensearch.test.OpenSearchTestCase;

/**
 * The parsing half of {@link NativeCpuFeatures}. The detection half depends on the
 * host CPU, so what can be pinned here is that a real /proc/cpuinfo is read the way
 * the flag checks assume — a parser that quietly returns nothing degrades to "no
 * SIMD support", which is safe but silently gives up the vectorized library.
 */
public class NativeCpuFeaturesTests extends OpenSearchTestCase {

    /** An x86_64 /proc/cpuinfo names the field "flags". */
    public void testParsesX86FlagsField() {
        Set<String> flags = NativeCpuFeatures.parseCpuFlags(
            Stream.of(
                "processor\t: 0",
                "model name\t: Intel(R) Xeon(R) Platinum 8375C CPU @ 2.90GHz",
                "flags\t\t: fpu vme de pse tsc avx avx2 fma f16c popcnt avx512f avx512dq avx512cd avx512bw avx512vl",
                "bugs\t\t: spectre_v1"
            )
        );

        assertTrue(flags.contains("avx2"));
        assertTrue(flags.contains("avx512vl"));
        assertFalse("only the flags field is parsed", flags.contains("intel(r)"));
        assertFalse(flags.contains("sve"));
    }

    /** An aarch64 /proc/cpuinfo names the same thing "Features". */
    public void testParsesAarch64FeaturesField() {
        Set<String> flags = NativeCpuFeatures.parseCpuFlags(
            Stream.of("processor\t: 0", "BogoMIPS\t: 2100.00", "Features\t: fp asimd evtstrm aes sha1 sve svebf16", "CPU architecture: 8")
        );

        assertTrue(flags.contains("sve"));
        assertFalse(flags.contains("avx2"));
    }

    /** Flags are matched case-insensitively against the lowercase names cmake uses. */
    public void testFlagsAreLowercased() {
        assertTrue(NativeCpuFeatures.parseCpuFlags(Stream.of("Features\t: SVE")).contains("sve"));
    }

    /**
     * The union across cores, not just the first core's line: a kernel that repeats
     * the field per processor must not lose the flags of the later ones.
     */
    public void testCollectsFlagsFromEveryCore() {
        Set<String> flags = NativeCpuFeatures.parseCpuFlags(
            Stream.of("processor\t: 0", "flags\t\t: fpu avx2", "processor\t: 1", "flags\t\t: fpu avx2 avx512f")
        );

        assertEquals(Set.of("fpu", "avx2", "avx512f"), flags);
    }

    /** A cpuinfo with no flags field at all means no SIMD, not a failure. */
    public void testMissingFieldYieldsNoFlags() {
        assertTrue(NativeCpuFeatures.parseCpuFlags(Stream.of("processor\t: 0", "vendor_id\t: GenuineIntel")).isEmpty());
        assertTrue(NativeCpuFeatures.parseCpuFlags(Stream.of("", "not a field at all")).isEmpty());
    }

    /**
     * Whatever this host is, exactly one answer per tier and never a claim of two
     * architectures at once.
     */
    public void testHostDetectionIsSelfConsistent() {
        if (NativeCpuFeatures.supportsAvx512()) {
            assertTrue("nsparse's avx512 variant is also compiled with -mavx2", NativeCpuFeatures.supportsAvx2());
        }
        assertFalse(
            "no CPU has both AVX and SVE",
            NativeCpuFeatures.supportsSve() && (NativeCpuFeatures.supportsAvx2() || NativeCpuFeatures.supportsAvx512())
        );
    }
}
