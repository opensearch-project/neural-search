/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.jni;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.extern.log4j.Log4j2;

/**
 * The instruction-set extensions the running CPU actually supports.
 * <p>
 * A distribution build ships every SIMD variant of the native library, so the name
 * a variant was built under is no longer proof that this host can run it. Loading
 * one the CPU lacks is not a slow path -- it is SIGILL on the first vectorized
 * call, which takes the node down rather than failing the load -- so the variant
 * has to be picked from the CPU, not from what happens to be on disk.
 * <p>
 * Detection is Linux-only, by reading {@code /proc/cpuinfo}. There is no portable
 * way to ask this from Java, and the alternatives are worse under the security
 * manager: {@code sysctl} needs an exec, and {@code jdk.incubator.vector} needs
 * module flags the plugin does not control. macOS and Windows therefore report no
 * features at all, which is consistent with jni/cmake/init-nsparse.cmake building
 * only the generic variant there anyway.
 */
@Log4j2
final class NativeCpuFeatures {

    /** Flags nsparse's avx2 variant is compiled with (see nsparse/CMakeLists.txt). */
    private static final Set<String> AVX2_FLAGS = Set.of("avx2", "fma", "f16c", "popcnt");

    /** The avx2 set plus the avx512 subsets nsparse's avx512 variant is compiled with. */
    private static final Set<String> AVX512_FLAGS = Stream.concat(
        AVX2_FLAGS.stream(),
        Stream.of("avx512f", "avx512cd", "avx512vl", "avx512dq", "avx512bw")
    ).collect(Collectors.toUnmodifiableSet());

    private static final Set<String> SVE_FLAGS = Set.of("sve");

    /**
     * Read once: /proc/cpuinfo cannot change under a running process, and the read
     * needs a privileged block that is not worth repeating per query.
     */
    private static final Set<String> FLAGS = readCpuFlags();

    static boolean supportsAvx2() {
        return FLAGS.containsAll(AVX2_FLAGS);
    }

    static boolean supportsAvx512() {
        return FLAGS.containsAll(AVX512_FLAGS);
    }

    static boolean supportsSve() {
        return FLAGS.containsAll(SVE_FLAGS);
    }

    private static Set<String> readCpuFlags() {
        if (!System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("linux")) {
            return Collections.emptySet();
        }
        Path cpuinfo = Path.of("/proc/cpuinfo");
        return AccessController.doPrivileged((PrivilegedAction<Set<String>>) () -> {
            try (Stream<String> lines = Files.lines(cpuinfo)) {
                return parseCpuFlags(lines);
            } catch (Exception e) {
                // Not fatal: an unreadable /proc/cpuinfo only costs the SIMD variants,
                // and the generic library is always a valid fallback.
                log.warn("Could not read {}; assuming no SIMD support. [{}]", cpuinfo, e.getMessage());
                return Collections.<String>emptySet();
            }
        });
    }

    /**
     * Collects the flag names from every {@code flags} (x86) or {@code Features}
     * (arm64) line.
     * <p>
     * The union across cores, rather than the first core's line, is deliberate:
     * on a heterogeneous CPU the union would claim support the scheduler cannot
     * guarantee -- but /proc/cpuinfo on such hosts already reports the same flags
     * for every core, and taking the first line silently loses everything on a
     * kernel that formats the field differently.
     */
    static Set<String> parseCpuFlags(Stream<String> lines) {
        return lines.map(line -> line.split(":", 2)).filter(parts -> parts.length == 2).filter(parts -> {
            String field = parts[0].trim();
            return field.equals("flags") || field.equals("Features");
        })
            .flatMap(parts -> Arrays.stream(parts[1].trim().split("\\s+")))
            .filter(flag -> !flag.isEmpty())
            .map(flag -> flag.toLowerCase(Locale.ROOT))
            .collect(Collectors.toUnmodifiableSet());
    }

    private NativeCpuFeatures() {}
}
