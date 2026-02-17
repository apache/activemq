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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

import static org.junit.Assert.*;

public class IOHelperTest {

    private Path baseDir;

    @Before
    public void setUp() throws IOException {
        baseDir = Paths.get("target", "iohelper-test").toAbsolutePath();
        if (Files.exists(baseDir)) {
            // best effort cleanup
            IOHelper.delete(new File(baseDir.toString()));
        }
        Files.createDirectories(baseDir);
    }

    @After
    public void tearDown() throws IOException {
        if (Files.exists(baseDir)) {
            IOHelper.delete(new File(baseDir.toString()));
        }
    }

    @Test
    public void testMoveFileNioCreated() throws Exception {
        Path srcDir = baseDir.resolve("src");
        Path dstDir = baseDir.resolve("dst");
        Files.createDirectories(srcDir);
        Files.createDirectories(dstDir);

        Path srcFile = srcDir.resolve("testfile.txt");
        Files.writeString(srcFile, "hello world", StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        File src = srcFile.toFile();
        File targetDir = dstDir.toFile();

        IOHelper.moveFile(src, targetDir);

        Path moved = dstDir.resolve("testfile.txt");
        assertTrue("moved file should exist", Files.exists(moved));
        assertEquals("content", "hello world", Files.readString(moved));
        assertFalse("original should not exist after move", Files.exists(srcFile));
    }

    @Test
    public void testMoveFileToDirectory() throws Exception {
        final Path srcDir = baseDir.resolve("src-dir");
        final Path dstDir = baseDir.resolve("dst-dir");
        Files.createDirectories(srcDir);
        Files.createDirectories(dstDir);

        final Path srcFile = srcDir.resolve("file.txt");
        Files.writeString(srcFile, "dir move test", StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        IOHelper.moveFile(srcFile.toFile(), dstDir.toFile());

        assertTrue("moved file should exist in target dir", Files.exists(dstDir.resolve("file.txt")));
        assertFalse("source should not exist", Files.exists(srcFile));
    }

    @Test
    public void testDeleteFileNonBlocking() throws Exception {
        final Path f = baseDir.resolve("nonblocking.txt");
        Files.writeString(f, "test", StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        final File file = f.toFile();

        final boolean result = IOHelper.deleteFileNonBlocking(file);

        assertTrue("file should be deleted or scheduled", result || !file.exists());
    }

    @Test
    public void testDeleteFileNonBlockingDirectory() throws Exception {
        final Path dir = baseDir.resolve("nonblocking-dir");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("child.txt"), "test", StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        final File file = dir.toFile();

        final boolean result = IOHelper.deleteFileNonBlocking(file);

        // Directory may be deleted immediately or scheduled for async cleanup
        assertTrue("dir deletion result", result || !file.exists() || Objects.requireNonNull(file.listFiles()).length == 0);
    }

    @Test
    public void testRenameFile() throws Exception {
        final Path srcFile = baseDir.resolve("rename-src.txt");
        final Path destFile = baseDir.resolve("rename-dest.txt");
        Files.writeString(srcFile, "rename test", StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        IOHelper.renameFile(srcFile.toFile(), destFile.toFile());

        assertTrue("renamed file should exist", Files.exists(destFile));
        assertEquals("content preserved", "rename test", Files.readString(destFile));
        assertFalse("source should not exist after rename", Files.exists(srcFile));
    }

    @Test(expected = IOException.class)
    public void testRenameFileSourceDoesNotExist() throws Exception {
        final File src = baseDir.resolve("does-not-exist.txt").toFile();
        final File dest = baseDir.resolve("dest.txt").toFile();

        IOHelper.renameFile(src, dest);
    }

    @Test
    public void testIsWindows() {
        // Just verify the method works without exceptions
        final boolean isWindows = IOHelper.isWindows();
        final String os = System.getProperty("os.name", "").toLowerCase();
        assertEquals("isWindows detection", os.contains("win"), isWindows);
    }
}
