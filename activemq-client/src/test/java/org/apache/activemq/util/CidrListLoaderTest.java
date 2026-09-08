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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CidrListLoaderTest {

    private Path dir;
    private String savedConf;

    @Before
    public void setUp() throws Exception {
        dir = Files.createDirectories(Path.of("target", "cidr-list-loader-test", Long.toString(System.nanoTime())));
        savedConf = System.getProperty("activemq.conf");
    }

    @After
    public void tearDown() {
        if (savedConf == null) {
            System.clearProperty("activemq.conf");
        } else {
            System.setProperty("activemq.conf", savedConf);
        }
    }

    private Path write(String name, String content) throws Exception {
        return Files.writeString(dir.resolve(name), content, StandardCharsets.UTF_8);
    }

    private static String uri(Path path) {
        return "file:" + path.toAbsolutePath();
    }

    @Test
    public void testCommaSeparatedValueCountsInvalidEntries() {
        var list = CidrListLoader.load("10.0.0.0/8, 192.168.1.0/24 ,bogus,,", "test");
        assertEquals(2, list.cidrs().size());
        assertEquals(1, list.invalidCount());
        assertEquals("10.0.0.0/8", list.cidrs().get(0).cidr());
    }

    @Test
    public void testNullOrBlankIsEmpty() {
        assertTrue(CidrListLoader.load(null, "test").cidrs().isEmpty());
        assertTrue(CidrListLoader.load("   ", "test").cidrs().isEmpty());
        assertEquals(0, CidrListLoader.load("", "test").invalidCount());
    }

    @Test
    public void testFileWithCommentsBlanksAndOneBadLine() throws Exception {
        var file = write("allow.txt", "# leading comment\n\n10.0.0.0/8   # trailing comment\n192.168.1.0/24\nnot-a-cidr\n   \n2001:db8::/32\n");
        var list = CidrListLoader.load(uri(file), "test");
        assertEquals(3, list.cidrs().size());
        assertEquals(1, list.invalidCount());
    }

    @Test
    public void testFileUriWithTripleSlash() throws Exception {
        var file = write("deny.txt", "10.0.0.0/8\n");
        var list = CidrListLoader.load("file://" + file.toAbsolutePath(), "test");
        assertEquals(1, list.cidrs().size());
    }

    @Test
    public void testMacroExpansion() throws Exception {
        System.setProperty("activemq.conf", dir.toAbsolutePath().toString());
        write("allow.txt", "10.0.0.0/8\n172.16.0.0/12\n");
        var list = CidrListLoader.load("file:${activemq.conf}/allow.txt", "test");
        assertEquals(2, list.cidrs().size());
    }

    @Test
    public void testUnsupportedMacroRejected() {
        var e = assertThrows(IllegalArgumentException.class,
                () -> CidrListLoader.load("file:${user.home}/allow.txt", "test"));
        assertTrue(e.getMessage(), e.getMessage().contains("Unsupported macro"));
    }

    @Test
    public void testUnsetMacroPropertyRejected() {
        System.clearProperty("activemq.conf");
        assertThrows(IllegalArgumentException.class,
                () -> CidrListLoader.load("file:${activemq.conf}/allow.txt", "test"));
    }

    @Test
    public void testOnlyFileSchemeAccepted() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> CidrListLoader.load("http://example.com/allow.txt", "test"));
        // an unknown scheme without a slash is treated as a CIDR list and simply yields invalid entries
        var list = CidrListLoader.load("classpath:allow.txt", "test");
        assertEquals(1, list.invalidCount());
    }

    @Test
    public void testAuthorityQueryAndFragmentRejected() throws Exception {
        var file = write("allow.txt", "10.0.0.0/8\n");
        assertThrows(IllegalArgumentException.class, () -> CidrListLoader.load("file://remotehost" + file.toAbsolutePath(), "test"));
        assertThrows(IllegalArgumentException.class, () -> CidrListLoader.load(uri(file) + "?x=1", "test"));
        assertThrows(IllegalArgumentException.class, () -> CidrListLoader.load(uri(file) + "#frag", "test"));
    }

    @Test
    public void testParentSegmentRejected() throws Exception {
        write("allow.txt", "10.0.0.0/8\n");
        var inner = Files.createDirectories(dir.resolve("inner"));
        System.setProperty("activemq.conf", inner.toAbsolutePath().toString());
        assertThrows(IllegalArgumentException.class,
                () -> CidrListLoader.load("file:${activemq.conf}/../allow.txt", "test"));
        assertThrows(IllegalArgumentException.class,
                () -> CidrListLoader.load(uri(inner) + "/../allow.txt", "test"));
    }

    @Test
    public void testMissingFileRejected() {
        var e = assertThrows(IllegalArgumentException.class,
                () -> CidrListLoader.load(uri(dir.resolve("nope.txt")), "test"));
        assertTrue(e.getMessage(), e.getMessage().contains("not a readable regular file"));
    }

    @Test
    public void testTooManyEntriesRejected() throws Exception {
        var content = IntStream.range(0, CidrListLoader.MAX_ENTRIES + 1)
                .mapToObj(i -> "10.0.0.1/32\n").collect(Collectors.joining());
        var file = write("huge.txt", content);
        var e = assertThrows(IllegalArgumentException.class, () -> CidrListLoader.load(uri(file), "test"));
        assertTrue(e.getMessage(), e.getMessage().contains("more than " + CidrListLoader.MAX_ENTRIES));
    }

    @Test
    public void testFileOverSizeLimitRejected() throws Exception {
        var file = dir.resolve("big.txt");
        try (var raf = new RandomAccessFile(file.toFile(), "rw")) {
            raf.setLength(CidrListLoader.MAX_FILE_BYTES + 1);
        }
        var e = assertThrows(IllegalArgumentException.class, () -> CidrListLoader.load(uri(file), "test"));
        assertTrue(e.getMessage(), e.getMessage().contains("larger than"));
    }
}
