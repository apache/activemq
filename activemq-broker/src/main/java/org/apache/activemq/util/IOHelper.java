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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Stack;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Collection of File and Folder utility methods.
 */
public final class IOHelper {

    private static final Logger LOG = LoggerFactory.getLogger(IOHelper.class);

    protected static final int MAX_DIR_NAME_LENGTH;
    protected static final int MAX_FILE_NAME_LENGTH;
    private static final int DEFAULT_BUFFER_SIZE = 4096;
    private static final boolean IS_WINDOWS = System.getProperty("os.name", "").toLowerCase().contains("win");

    // Async cleanup for files that couldn't be deleted immediately (Windows file locking)
    private static final Queue<File> PENDING_DELETES = new ConcurrentLinkedQueue<>();
    private static final ScheduledExecutorService CLEANUP_EXECUTOR;

    static {
        MAX_DIR_NAME_LENGTH = Integer.getInteger("MaximumDirNameLength", 200);
        MAX_FILE_NAME_LENGTH = Integer.getInteger("MaximumFileNameLength", 64);

        // Only start cleanup thread on Windows where file locking is an issue
        if (IS_WINDOWS) {
            CLEANUP_EXECUTOR = Executors.newSingleThreadScheduledExecutor(r -> {
                final Thread t = new Thread(r, "IOHelper-AsyncCleanup");
                t.setDaemon(true);
                return t;
            });
            CLEANUP_EXECUTOR.scheduleWithFixedDelay(IOHelper::processAsyncDeletes, 5, 5, TimeUnit.SECONDS);
        } else {
            CLEANUP_EXECUTOR = null;
        }
    }

    private IOHelper() {
    }

    /**
     * Process pending async deletes. Called periodically on Windows.
     */
    private static void processAsyncDeletes() {
        int processed = 0;
        File file;
        while ((file = PENDING_DELETES.poll()) != null && processed < 100) {
            processed++;
            if (file.exists()) {
                if (file.delete()) {
                    LOG.debug("Async cleanup: deleted {}", file);
                } else {
                    // Still locked, re-queue for later
                    PENDING_DELETES.offer(file);
                }
            }
        }
    }

    /**
     * Schedule a file for async deletion. Used when immediate deletion fails on Windows.
     * The file will be deleted in the background when it becomes unlocked.
     */
    public static void scheduleAsyncDelete(File file) {
        if (file != null && file.exists()) {
            if (IS_WINDOWS && CLEANUP_EXECUTOR != null) {
                PENDING_DELETES.offer(file);
                LOG.debug("Scheduled async delete for: {}", file);
            } else {
                // On non-Windows, just use deleteOnExit
                file.deleteOnExit();
            }
        }
    }

    /**
     * Non-blocking file deletion. Tries to delete once, and if it fails,
     * schedules async cleanup instead of blocking with retries.
     *
     * This is safe to call from synchronized blocks as it never sleeps or retries.
     *
     * @param file the file to delete
     * @return true if deleted immediately, false if scheduled for async cleanup
     */
    public static boolean deleteFileNonBlocking(File file) {
        if (file == null || !file.exists()) {
            return true;
        }

        // For directories, try to delete children first (non-blocking)
        if (file.isDirectory()) {
            final File[] children = file.listFiles();
            if (children != null) {
                for (final File child : children) {
                    deleteFileNonBlocking(child);
                }
            }
        }

        // Single delete attempt - no retry
        if (file.delete()) {
            return true;
        }

        // Failed to delete - schedule for async cleanup
        scheduleAsyncDelete(file);
        return false;
    }

    /**
     * Check if we're running on Windows.
     */
    public static boolean isWindows() {
        return IS_WINDOWS;
    }

    public static String getDefaultDataDirectory() {
        return getDefaultDirectoryPrefix() + "activemq-data";
    }

    public static String getDefaultStoreDirectory() {
        return getDefaultDirectoryPrefix() + "amqstore";
    }

    /**
     * Allows a system property to be used to overload the default data
     * directory which can be useful for forcing the test cases to use a target/
     * prefix
     */
    public static String getDefaultDirectoryPrefix() {
        try {
            return System.getProperty("org.apache.activemq.default.directory.prefix", "");
        } catch (Exception e) {
            return "";
        }
    }

    /**
     * Converts any string into a string that is safe to use as a file name. The
     * result will only include ascii characters and numbers, and the "-","_",
     * and "." characters.
     *
     * @param name
     * @return safe name of the directory
     */
    public static String toFileSystemDirectorySafeName(String name) {
        return toFileSystemSafeName(name, true, MAX_DIR_NAME_LENGTH);
    }

    public static String toFileSystemSafeName(String name) {
        return toFileSystemSafeName(name, false, MAX_FILE_NAME_LENGTH);
    }

    /**
     * Converts any string into a string that is safe to use as a file name. The
     * result will only include ascii characters and numbers, and the "-","_",
     * and "." characters.
     *
     * @param name
     * @param dirSeparators
     * @param maxFileLength
     * @return file system safe name
     */
    public static String toFileSystemSafeName(String name, boolean dirSeparators, int maxFileLength) {
        int size = name.length();
        StringBuilder rc = new StringBuilder(size * 2);
        for (int i = 0; i < size; i++) {
            char c = name.charAt(i);
            boolean valid = c >= 'a' && c <= 'z';
            valid = valid || (c >= 'A' && c <= 'Z');
            valid = valid || (c >= '0' && c <= '9');
            valid = valid || (c == '_') || (c == '-') || (c == '.') || (c == '#') || (dirSeparators && ((c == '/') || (c == '\\')));

            if (valid) {
                rc.append(c);
            } else {
                // Encode the character using hex notation
                rc.append('#');
                rc.append(HexSupport.toHexFromInt(c, true));
            }
        }
        String result = rc.toString();
        if (result.length() > maxFileLength) {
            result = result.substring(result.length() - maxFileLength, result.length());
        }
        return result;
    }

    public static boolean delete(File top) {
        boolean result = true;
        Stack<File> files = new Stack<File>();
        // Add file to the stack to be processed...
        files.push(top);
        // Process all files until none remain...
        while (!files.isEmpty()) {
            File file = files.pop();
            if (file.isDirectory()) {
                File list[] = file.listFiles();
                if (list == null || list.length == 0) {
                    // The current directory contains no entries...
                    // delete directory and continue...
                    result &= file.delete();
                } else {
                    // Add back the directory since it is not empty....
                    // and when we process it again it will be empty and can be
                    // deleted safely...
                    files.push(file);
                    for (File dirFile : list) {
                        if (dirFile.isDirectory()) {
                            // Place the directory on the stack...
                            files.push(dirFile);
                        } else {
                            // This is a simple file, delete it...
                            result &= dirFile.delete();
                        }
                    }
                }
            } else {
                // This is a simple file, delete it...
                result &= file.delete();
            }
        }
        return result;
    }

    public static boolean deleteFile(File fileToDelete) {
        if (fileToDelete == null || !fileToDelete.exists()) {
            return true;
        }
        boolean result = deleteChildren(fileToDelete);
        result &= fileToDelete.delete();
        return result;
    }

    public static boolean deleteChildren(File parent) {
        if (parent == null || !parent.exists()) {
            return false;
        }
        boolean result = true;
        if (parent.isDirectory()) {
            File[] files = parent.listFiles();
            if (files == null) {
                result = false;
            } else {
                for (int i = 0; i < files.length; i++) {
                    File file = files[i];
                    if (file.getName().equals(".") || file.getName().equals("..")) {
                        continue;
                    }
                    if (file.isDirectory()) {
                        result &= deleteFile(file);
                    } else {
                        result &= file.delete();
                    }
                }
            }
        }

        return result;
    }

    /**
     * Rename a file to a new name/path. Non-blocking on failure.
     * Strategy: rename -> NIO move -> copy + async delete of source.
     *
     * @param src the source file
     * @param dest the destination file (full path with new name)
     */
    public static void renameFile(File src, File dest) throws IOException {
        if (src == null) {
            throw new IOException("Source file is null");
        }
        if (dest == null) {
            throw new IOException("Destination file is null");
        }
        if (!src.exists()) {
            return;
        }

        final Path sourcePath = src.toPath();
        final Path targetPath = dest.toPath();

        // Fast path: try rename
        try {
            if (src.renameTo(dest)) {
                return;
            }
        } catch (Exception e) {
            // Fall through to NIO move
        }

        // Try NIO move
        try {
            Files.move(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
            return;
        } catch (IOException e) {
            // Fall through to copy+delete
        }

        // Copy + async delete as last resort
        Files.copy(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.COPY_ATTRIBUTES);
        deleteFileNonBlocking(src);
    }

    /**
     * Move a file to a target directory. Non-blocking - uses async cleanup for source on Windows.
     * Strategy: rename -> NIO move -> copy + async delete of source.
     *
     * @param src the source file to move
     * @param targetDirectory the target directory (must be a directory)
     */
    public static void moveFile(File src, File targetDirectory) throws IOException {
        if (src == null) {
            throw new IOException("Source file is null");
        }
        if (targetDirectory == null) {
            throw new IOException("Target directory is null");
        }
        mkdirs(targetDirectory);
        final File dest = new File(targetDirectory, src.getName());
        final Path sourcePath = src.toPath();
        final Path targetPath = dest.toPath();

        // Fast path: try rename (works if same filesystem and no locks)
        try {
            if (src.renameTo(dest)) {
                return;
            }
        } catch (Exception e) {
            // Fall through to NIO move
        }

        // Try NIO move
        try {
            Files.move(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
            return;
        } catch (IOException e) {
            // Fall through to copy+delete
        }

        // Copy + async delete as last resort
        Files.copy(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.COPY_ATTRIBUTES);
        // Copy succeeded, schedule async cleanup of source (non-blocking)
        deleteFileNonBlocking(src);
    }

    public static void moveFiles(File srcDirectory, File targetDirectory, FilenameFilter filter) throws IOException {
        if (!srcDirectory.isDirectory()) {
            throw new IOException("source is not a directory");
        }

        if (targetDirectory.exists() && !targetDirectory.isDirectory()) {
            throw new IOException("target exists and is not a directory");
        } else {
            mkdirs(targetDirectory);
        }

        List<File> filesToMove = new ArrayList<File>();
        getFiles(srcDirectory, filesToMove, filter);

        for (File file : filesToMove) {
            if (!file.isDirectory()) {
                moveFile(file, targetDirectory);
            }
        }
    }

    public static void copyFile(File src, File dest) throws IOException {
        copyFile(src, dest, null);
    }

    public static void copyFile(File src, File dest, FilenameFilter filter) throws IOException {
        if (src.getCanonicalPath().equals(dest.getCanonicalPath()) == false) {
            if (src.isDirectory()) {

                mkdirs(dest);
                List<File> list = getFiles(src, filter);
                for (File f : list) {
                    if (f.isFile()) {
                        File target = new File(getCopyParent(src, dest, f), f.getName());
                        copySingleFile(f, target);
                    }
                }

            } else if (dest.isDirectory()) {
                mkdirs(dest);
                File target = new File(dest, src.getName());
                copySingleFile(src, target);
            } else {
                copySingleFile(src, dest);
            }
        }
    }

    static File getCopyParent(File from, File to, File src) {
        File result = null;
        File parent = src.getParentFile();
        String fromPath = from.getAbsolutePath();
        if (parent.getAbsolutePath().equals(fromPath)) {
            // one level down
            result = to;
        } else {
            String parentPath = parent.getAbsolutePath();
            String path = parentPath.substring(fromPath.length());
            result = new File(to.getAbsolutePath() + File.separator + path);
        }
        return result;
    }

    static List<File> getFiles(File dir, FilenameFilter filter) {
        List<File> result = new ArrayList<File>();
        getFiles(dir, result, filter);
        return result;
    }

    static void getFiles(File dir, List<File> list, FilenameFilter filter) {
        if (!list.contains(dir)) {
            list.add(dir);
            String[] fileNames = dir.list(filter);
            for (int i = 0; i < fileNames.length; i++) {
                File f = new File(dir, fileNames[i]);
                if (f.isFile()) {
                    list.add(f);
                } else {
                    getFiles(dir, list, filter);
                }
            }
        }
    }

    public static void copySingleFile(File src, File dest) throws IOException {
        FileInputStream fileSrc = new FileInputStream(src);
        FileOutputStream fileDest = new FileOutputStream(dest);
        copyInputStream(fileSrc, fileDest);
    }

    public static void copyInputStream(InputStream in, OutputStream out) throws IOException {
        try {
            byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
            int len = in.read(buffer);
            while (len >= 0) {
                out.write(buffer, 0, len);
                len = in.read(buffer);
            }
        } finally {
            in.close();
            out.close();
        }
    }

    public static int getMaxDirNameLength() {
        return MAX_DIR_NAME_LENGTH;
    }

    public static int getMaxFileNameLength() {
        return MAX_FILE_NAME_LENGTH;
    }

    public static void mkdirs(File dir) throws IOException {
        if (dir.exists()) {
            if (!dir.isDirectory()) {
                throw new IOException("Failed to create directory '" + dir +
                                      "', regular file already existed with that name");
            }

        } else {
            if (!dir.mkdirs()) {
                if ( dir.exists() && dir.isDirectory() ) {
                    // Directory created in parallel
                    return;
                }
                throw new IOException("Failed to create directory '" + dir + "'");
            }
        }
    }
}
