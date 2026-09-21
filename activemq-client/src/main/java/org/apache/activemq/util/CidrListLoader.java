/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads a CIDR allow or deny list from a transport connector setting. The value
 * is either a comma separated list of CIDR blocks or a {@code file:} URI to a
 * text file with one CIDR block per line ({@code #} starts a comment). Only the
 * {@code file} scheme is accepted, and the URI may use the {@code ${activemq.conf}}
 * and {@code ${activemq.data}} macros, which expand from system properties.
 *
 * <p>Malformed CIDR entries are logged and skipped, and their number is reported
 * on the result. Anything wrong with the file reference itself (scheme, path
 * traversal, missing file, more than {@value #MAX_ENTRIES} entries or more than
 * {@value #MAX_FILE_BYTES} bytes) is a configuration error and throws.
 */
public final class CidrListLoader {

    private static final Logger LOG = LoggerFactory.getLogger(CidrListLoader.class);

    public static final int MAX_ENTRIES = 100_000;
    public static final long MAX_FILE_BYTES = 10L * 1024 * 1024;

    private static final String FILE_PREFIX = "file:";
    private static final Map<String, String> MACROS = Map.of(
            "${activemq.conf}", "activemq.conf",
            "${activemq.data}", "activemq.data");

    private CidrListLoader() {
    }

    /** The parsed entries of one list plus the number of entries that were skipped. */
    public static final class CidrList {
        public static final CidrList EMPTY = new CidrList(Collections.emptyList(), 0);

        private final List<Cidr> cidrs;
        private final int invalidCount;

        CidrList(List<Cidr> cidrs, int invalidCount) {
            this.cidrs = Collections.unmodifiableList(cidrs);
            this.invalidCount = invalidCount;
        }

        public List<Cidr> cidrs() {
            return cidrs;
        }

        public int invalidCount() {
            return invalidCount;
        }
    }

    /**
     * @param value        comma separated CIDR blocks, a {@code file:} URI, or null/blank for none
     * @param locationHint names the setting in log messages, e.g. "openwire allowList"
     */
    public static CidrList load(String value, String locationHint) {
        if (value == null || value.isBlank()) {
            return CidrList.EMPTY;
        }
        var trimmed = value.trim();
        if (trimmed.regionMatches(true, 0, FILE_PREFIX, 0, FILE_PREFIX.length())) {
            return loadFile(trimmed, locationHint);
        }
        // "://" never appears in a CIDR block (IPv6 uses "::/"), so this is some other URI scheme
        if (trimmed.contains("://")) {
            throw new IllegalArgumentException("Only file: URIs are supported for CIDR lists (" + locationHint + "): " + trimmed);
        }
        return parse(List.of(trimmed.split(",")), locationHint);
    }

    static CidrList loadFile(String fileUri, String locationHint) {
        var path = resolveFile(fileUri);
        try {
            var size = Files.size(path);
            if (size > MAX_FILE_BYTES) {
                throw new IllegalArgumentException("CIDR list file " + path + " is " + size
                        + " bytes, larger than the " + MAX_FILE_BYTES + " byte limit (" + locationHint + ")");
            }
            var entries = new ArrayList<String>();
            try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    var comment = line.indexOf('#');
                    var entry = (comment >= 0 ? line.substring(0, comment) : line).trim();
                    if (entry.isEmpty()) {
                        continue;
                    }
                    if (entries.size() >= MAX_ENTRIES) {
                        throw new IllegalArgumentException("CIDR list file " + path + " has more than "
                                + MAX_ENTRIES + " entries (" + locationHint + ")");
                    }
                    entries.add(entry);
                }
            }
            return parse(entries, locationHint + " (" + path + ")");
        } catch (IOException e) {
            throw new IllegalArgumentException("Cannot read CIDR list file " + path + " (" + locationHint + ")", e);
        }
    }

    /**
     * Expands the permitted macros, insists on the {@code file} scheme with no
     * authority, query or fragment, refuses any {@code ..} segment, and when a
     * macro was used requires the file to stay under that macro's directory.
     */
    static Path resolveFile(String fileUri) {
        Path macroRoot = null;
        var expanded = fileUri;
        var start = expanded.indexOf("${");
        while (start >= 0) {
            var end = expanded.indexOf('}', start);
            if (end < 0) {
                throw new IllegalArgumentException("Unterminated macro in CIDR list location: " + fileUri);
            }
            var macro = expanded.substring(start, end + 1);
            var property = MACROS.get(macro);
            if (property == null) {
                throw new IllegalArgumentException("Unsupported macro " + macro + " in CIDR list location: " + fileUri
                        + " (only " + MACROS.keySet() + " are permitted)");
            }
            var replacement = System.getProperty(property);
            if (replacement == null || replacement.isBlank()) {
                throw new IllegalArgumentException("System property " + property + " is not set, needed by CIDR list location: " + fileUri);
            }
            if (macroRoot == null) {
                macroRoot = Path.of(replacement).toAbsolutePath().normalize();
            }
            expanded = expanded.substring(0, start) + replacement + expanded.substring(end + 1);
            start = expanded.indexOf("${");
        }

        if (!expanded.regionMatches(true, 0, FILE_PREFIX, 0, FILE_PREFIX.length())) {
            throw new IllegalArgumentException("Only file: URIs are supported for CIDR lists: " + fileUri);
        }
        var pathPart = expanded.substring(FILE_PREFIX.length());
        if (pathPart.startsWith("//")) {
            // file:///path is fine, file://host/path is not
            if (pathPart.length() < 3 || pathPart.charAt(2) != '/') {
                throw new IllegalArgumentException("file: URI with an authority is not supported for CIDR lists: " + fileUri);
            }
            pathPart = pathPart.substring(2);
        }
        if (pathPart.indexOf('?') >= 0 || pathPart.indexOf('#') >= 0) {
            throw new IllegalArgumentException("file: URI with a query or fragment is not supported for CIDR lists: " + fileUri);
        }
        for (var segment : pathPart.split("[/\\\\]")) {
            if ("..".equals(segment)) {
                throw new IllegalArgumentException("CIDR list location must not contain '..': " + fileUri);
            }
        }

        var path = Path.of(pathPart).toAbsolutePath().normalize();
        if (macroRoot != null && !path.startsWith(macroRoot)) {
            throw new IllegalArgumentException("CIDR list location escapes its macro directory " + macroRoot + ": " + fileUri);
        }
        if (!Files.isRegularFile(path) || !Files.isReadable(path)) {
            throw new IllegalArgumentException("CIDR list file is not a readable regular file: " + path);
        }
        return path;
    }

    /** Parses entries one at a time so skipped entries can be counted; each failure logs a WARN. */
    static CidrList parse(List<String> entries, String locationHint) {
        var cidrs = new ArrayList<Cidr>(entries.size());
        var invalid = 0;
        for (var entry : entries) {
            var trimmed = entry.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            try {
                cidrs.add(CidrConverter.fromString(trimmed));
            } catch (IllegalArgumentException e) {
                invalid++;
                LOG.warn("Invalid CIDR string:{} from:{}", trimmed, locationHint);
            }
        }
        return new CidrList(cidrs, invalid);
    }
}
