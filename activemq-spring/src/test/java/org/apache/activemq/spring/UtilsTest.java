/*
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
package org.apache.activemq.spring;

import static org.apache.activemq.spring.Utils.CLASSPATH_PROTOCOL;
import static org.apache.activemq.spring.Utils.FILE_PROTOCOL;
import static org.apache.activemq.spring.Utils.REMOTE_FILE_PROTOCOL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.Set;
import java.util.concurrent.Callable;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;

public class UtilsTest {

    @Test
    public void testIsAllowLocalFile() {
        Set<String> localFileExamples = Set.of("/some/absolute/file", "relative/file", "file.txt");
        for (String localFile : localFileExamples) {
            assertTrue(Utils.isAllowFile(null, localFile));
            assertTrue(Utils.isAllowFile(Set.of(FILE_PROTOCOL), localFile));
            assertTrue(Utils.isAllowFile(Set.of(FILE_PROTOCOL, "ftp", "ssl"), localFile));

            assertFalse(Utils.isAllowFile(Set.of(CLASSPATH_PROTOCOL, "ftp", "ssl"), localFile));
            assertFalse(Utils.isAllowFile(Set.of(), localFile));
            assertFalse(Utils.isAllowFile(Set.of(""), localFile));   
        }

        // Test a remote file isn't allowed with only local
        // Check windows backward slashes as well
        Set<String> remoteFileExamples = Set.of("//some/remote/file", "\\\\remote\\file");
        for (String remoteFileName : remoteFileExamples) {
            assertTrue(Utils.isAllowFile(null, remoteFileName));
            // None of these should be allowed as remote-file isn't included with FILE
            assertFalse(Utils.isAllowFile(Set.of(FILE_PROTOCOL), remoteFileName));
            assertFalse(Utils.isAllowFile(Set.of(FILE_PROTOCOL, "ftp", "ssl"), remoteFileName));
        }

    }

    @Test
    public void testIsAllowRemoteFile() {

        // Test a remote file
        Set<String> remoteFileExamples = Set.of("//some/remote/file", "\\\\remote\\file");
        for (String remoteFileName : remoteFileExamples) {
            assertTrue(Utils.isAllowFile(null, remoteFileName));
            assertTrue(Utils.isAllowFile(Set.of(REMOTE_FILE_PROTOCOL), remoteFileName));
            assertTrue(Utils.isAllowFile(Set.of(REMOTE_FILE_PROTOCOL, "ftp", "ssl"), remoteFileName));
            assertFalse(Utils.isAllowFile(Set.of(CLASSPATH_PROTOCOL, "ftp", "ssl"), remoteFileName));
            assertFalse(Utils.isAllowFile(Set.of(), remoteFileName));
            assertFalse(Utils.isAllowFile(Set.of(""), remoteFileName));
        }

    }

    @Test
    public void testIsAllowClasspath() {
        assertTrue(Utils.isAllowClasspath(null));
        assertTrue(Utils.isAllowClasspath(Set.of(CLASSPATH_PROTOCOL)));
        assertTrue(Utils.isAllowClasspath(Set.of(CLASSPATH_PROTOCOL, "ftp", "ssl")));
        assertFalse(Utils.isAllowClasspath(Set.of(FILE_PROTOCOL, "ftp", "ssl")));
        assertFalse(Utils.isAllowClasspath(Set.of()));
        assertFalse(Utils.isAllowClasspath(Set.of("")));
    }

    @Test
    public void testValidateUrlAllowed() throws URISyntaxException {
        // not a qualified url so this throws an exception
        assertValidateUrlAllowedThrows("somefile.txt", Set.of(FILE_PROTOCOL));

        // Test File - Allowed
        Utils.validateUrlAllowed("file:/somefile.txt", Set.of(FILE_PROTOCOL));
        Utils.validateUrlAllowed("file:/somefile.txt", Set.of(FILE_PROTOCOL, CLASSPATH_PROTOCOL));
        Utils.validateUrlAllowed("file:some/other/file.txt", null);

        // Test File - Blocked
        assertValidateUrlAllowedThrows("file:somefile.txt", Set.of());
        assertValidateUrlAllowedThrows("file:some/other/file.txt", Set.of(""));
        assertValidateUrlAllowedThrows("file:some/other/file.txt", Set.of(CLASSPATH_PROTOCOL));

        // Test Remote File - Allowed
        Utils.validateUrlAllowed("file://somefile.txt", Set.of(REMOTE_FILE_PROTOCOL));
        Utils.validateUrlAllowed("file://somefile.txt", Set.of(FILE_PROTOCOL, REMOTE_FILE_PROTOCOL));
        Utils.validateUrlAllowed("file://some/other/file.txt", null);

        // Test File - Blocked
        assertValidateUrlAllowedThrows("file://somefile.txt", Set.of());
        assertValidateUrlAllowedThrows("file://some/other/file.txt", Set.of(""));
        assertValidateUrlAllowedThrows("file://some/other/file.txt", Set.of(FILE_PROTOCOL));

        // Test http - Allowed
        Utils.validateUrlAllowed("http://somefile.txt", Set.of("http"));
        Utils.validateUrlAllowed("http://somefile.txt", Set.of(FILE_PROTOCOL, "http"));
        Utils.validateUrlAllowed("http://some/other/file.txt", null);

        // Test http - Blocked
        assertValidateUrlAllowedThrows("http://somefile.txt", Set.of());
        assertValidateUrlAllowedThrows("http://some/other/file.txt", Set.of(""));
        assertValidateUrlAllowedThrows("http://some/other/file.txt", Set.of("ftp"));
    }

    private void assertValidateUrlAllowedThrows(String uriString, Set<String> allowedProtocols)
            throws URISyntaxException {
        try {
            Utils.validateUrlAllowed(uriString, allowedProtocols);
            fail("Should have failed with an exception");
        } catch (IllegalArgumentException ignored) {
            // expected
        }
    }

    // Test 1: Check file and remote file is NOT allowed to load, and does NOT exist
    @Test
    public void testResourceFromStringFile1() throws Exception {
        testResourceFromStringFile1(FILE_PROTOCOL, "doesNotExist", "file:doesNotExist");
    }

    @Test
    public void testResourceFromStringRemoteFile1() throws Exception {
        testResourceFromStringFile1(REMOTE_FILE_PROTOCOL, "//doesNotExist", "file://doesNotExist");
    }

    protected void testResourceFromStringFile1(String protocol, String url, String fqUrl) throws Exception {
        Resource resource;

        // file not allowed, only jar
        assertNotAllowed("URL [" + url + "] can't be found or is not allowed for loading resources",
                () -> Utils.resourceFromString(url, Set.of("jar")));
        // if classpath is allowed and not fully qualified, it will not find the file and fallback
        resource = Utils.resourceFromString(url, Set.of(CLASSPATH_PROTOCOL));
        assertTrue(resource instanceof ClassPathResource);
        assertFalse(resource.exists());
        // fully qualified fails regardless
        assertNotAllowed("URL [" + fqUrl + "] uses protocol '" + protocol + "' which is not allowed for loading URL resources",
                () -> Utils.resourceFromString(fqUrl, Set.of(CLASSPATH_PROTOCOL)));
        // Test Uri format, empty set not allowed
        assertNotAllowed("No protocols are allowed for loading resources.",
                () -> Utils.resourceFromString(fqUrl, Set.of()));

    }

    // Test 2: Check file and remote file is allowed to load, but it does not exist
    // This will throw an exception if classpath is not allowed or fully qualified uri,
    // otherwise it will fallback to try classpath if classpath is allowed
    @Test
    public void testResourceFromStringFile2() throws Exception {
        testResourceFromStringFile2(FILE_PROTOCOL, "doesNotExist", "file:doesNotExist");
    }

    @Test
    public void testResourceFromStringRemoteFile2() throws Exception {
        testResourceFromStringFile2(REMOTE_FILE_PROTOCOL, "//doesNotExist", "file://doesNotExist");
    }

    protected void testResourceFromStringFile2(String protocol, String url, String fqUrl) throws Exception {
        Resource resource;

        //classpath allowed so it will fallback
        resource = Utils.resourceFromString(url, Set.of(protocol, CLASSPATH_PROTOCOL));
        assertTrue(resource instanceof ClassPathResource);
        assertFalse(resource.exists());
        resource = Utils.resourceFromString(url, null);
        assertTrue(resource instanceof ClassPathResource);
        assertFalse(resource.exists());
        // single argument - default is null for allowed protocols so all are allowed
        resource = Utils.resourceFromString(url);
        assertTrue(resource instanceof ClassPathResource);
        assertFalse(resource.exists());
        // classpath not allowed so we get an exception as we can't find it
        assertNotAllowed("URL [" + url + "] can't be found or is not allowed for loading resources",
                () -> Utils.resourceFromString(url, Set.of(protocol)));
        resource = Utils.resourceFromString(fqUrl, Set.of(protocol));
        assertNotNull(resource);
        assertFalse(resource.exists());
    }

    // Test 3: Check file is NOT allowed to load, but it does exist
    @Test
    public void tetsResourceFromStringFile3() throws Exception {
        Resource resource;

        // Test using "jar" as allowed so we don't fallback
        assertNotAllowed("URL [src/test/resources/activemq.xml] can't be found or is not allowed for loading resources",
                () -> Utils.resourceFromString("src/test/resources/activemq.xml", Set.of("jar")));
        // classpath is allowed so it won't find the file and fallback to classpath
        resource = Utils.resourceFromString("src/test/resources/activemq.xml", Set.of(CLASSPATH_PROTOCOL));
        assertTrue(resource instanceof ClassPathResource);
        assertFalse(resource.exists());
        // empty allow list
        assertNotAllowed("No protocols are allowed for loading resources.",
                () -> Utils.resourceFromString("src/test/resources/activemq.xml", Set.of()));
        // Test Uri format - only classpath allowed
        assertNotAllowed("URL [file:src/test/resources/activemq.xml] uses protocol 'file' which is not allowed for loading URL resources",
                () -> Utils.resourceFromString("file:src/test/resources/activemq.xml", Set.of(CLASSPATH_PROTOCOL)));
    }

    // Test 4: Check file is both allowed and exists
    @Test
    public void testResourceFromStringFile4() throws Exception {
        Resource resource;

        resource = Utils.resourceFromString("src/test/resources/activemq.xml", Set.of(FILE_PROTOCOL));
        assertTrue(resource instanceof FileSystemResource);
        assertTrue(resource.exists());

        // Retry with file allowed using uri format
        resource = Utils.resourceFromString("file:src/test/resources/activemq.xml", Set.of(FILE_PROTOCOL));
        assertTrue(resource instanceof UrlResource);
        assertTrue(resource.exists());
    }

    // Test 1: Check classpath is NOT allowed to load, and does NOT exist
    @Test
    public void tetsResourceFromStringClasspath1() {
        // Check classpath is NOT allowed to load, and does NOT exist
        assertNotAllowed("URL [doesNotExist] can't be found or is not allowed for loading resources",
                () -> Utils.resourceFromString("doesNotExist", Set.of(FILE_PROTOCOL)));
        assertNotAllowed("URL [classpath:doesNotExist] uses protocol 'classpath' which is not allowed for loading URL resources",
                () -> Utils.resourceFromString("classpath:doesNotExist", Set.of(FILE_PROTOCOL)));
        // Test Uri format, empty set not allowed
        assertNotAllowed("No protocols are allowed for loading resources.",
                () -> Utils.resourceFromString("classpath:doesNotExist", Set.of()));
    }

    // Test 2: Check classpath is allowed to load, but it does not exist
    // This will return a classpath resource because file is tried first
    // but won't be found so it eventually returns a classpath resource
    @Test
    public void testResourceFromStringClasspath2() throws Exception {
        Resource resource;

        resource = Utils.resourceFromString("doesNotExist", Set.of(FILE_PROTOCOL, CLASSPATH_PROTOCOL));
        assertTrue(resource instanceof ClassPathResource);
        assertFalse(resource.exists());
        resource = Utils.resourceFromString("doesNotExist", Set.of(CLASSPATH_PROTOCOL));
        assertTrue(resource instanceof ClassPathResource);
        assertFalse(resource.exists());
        resource = Utils.resourceFromString("doesNotExist", null);
        assertTrue(resource instanceof ClassPathResource);
        assertFalse(resource.exists());
        // test single argument
        resource = Utils.resourceFromString("doesNotExist");
        assertTrue(resource instanceof ClassPathResource);
        assertFalse(resource.exists());
    }

    // Test 3: Check classpath is NOT allowed to load, but it does exist
    @Test
    public void testResourceFromStringClasspath3() {
        // This exists on the classpath but not allowed, only file is allowed
        assertNotAllowed("URL [activemq.xml] can't be found or is not allowed for loading resources",
                () -> Utils.resourceFromString("activemq.xml", Set.of(FILE_PROTOCOL)));
        // empty allow list
        assertNotAllowed("No protocols are allowed for loading resources.",
                () -> Utils.resourceFromString("activemq.xml", Set.of()));
        // Test Uri format - only file allowed
        assertNotAllowed("URL [classpath:activemq.xml] uses protocol 'classpath' which is not allowed for loading URL resources",
                () -> Utils.resourceFromString("classpath:activemq.xml", Set.of(FILE_PROTOCOL)));
    }

    @Test
    public void testResourceFromStringClasspath4() throws Exception {
        Resource resource;

        // Test 4: Check classpath is both allowed and exists
        resource = Utils.resourceFromString("activemq.xml", Set.of(CLASSPATH_PROTOCOL));
        assertTrue(resource instanceof ClassPathResource);
        assertTrue(resource.exists());
        // Retry with classpath allowed using uri format
        resource = Utils.resourceFromString("classpath:activemq.xml", Set.of(CLASSPATH_PROTOCOL));
        assertTrue(resource instanceof UrlResource);
        assertTrue(resource.exists());
    }

    // Test URIs not allowed
    @Test
    public void testResourceFromStringUri1() throws Exception {
        Resource resource;

        // none of these protocols are allowed
        assertNotAllowed("URL [file://invalid] uses protocol 'remote-file' which is not allowed for loading URL resources",
                () -> Utils.resourceFromString("file://invalid", Set.of(FILE_PROTOCOL,CLASSPATH_PROTOCOL)));
        assertNotAllowed("URL [file:\\\\invalid] uses protocol 'remote-file' which is not allowed for loading URL resources",
                () -> Utils.resourceFromString("file:\\\\invalid", Set.of(FILE_PROTOCOL,CLASSPATH_PROTOCOL)));
        assertNotAllowed("URL [http://invalid] uses protocol 'http' which is not allowed for loading URL resources",
                () -> Utils.resourceFromString("http://invalid", Set.of(FILE_PROTOCOL,CLASSPATH_PROTOCOL)));
        assertNotAllowed("URL [ftp://invalid] uses protocol 'ftp' which is not allowed for loading URL resources",
                () -> Utils.resourceFromString("ftp://invalid", Set.of(FILE_PROTOCOL,CLASSPATH_PROTOCOL)));
        assertNotAllowed("URL [jar:file:invalid.jar!/] uses protocol 'jar' which is not allowed for loading URL resources",
                () -> Utils.resourceFromString("jar:file:invalid.jar!/", Set.of(FILE_PROTOCOL,CLASSPATH_PROTOCOL)));
        assertNotAllowed("No protocols are allowed for loading resources.",
                () -> Utils.resourceFromString("http://invalid", Set.of()));
        // malformed
        try {
            // not allowed but should have malformed error before it even checks
            Utils.resourceFromString("http:", Set.of("http"));
            fail("should have exception");
        } catch (MalformedURLException e) {
            assertTrue(e.getCause() instanceof URISyntaxException);
        }

        // special edge case - "bad" is not a valid protocol so it skips the URI loading
        // and falls back to a classpath search, which is allowed. That will of course fail because
        // it's not a valid classpath entry
        resource = Utils.resourceFromString("bad://doesNotExist", Set.of(CLASSPATH_PROTOCOL));
        assertTrue(resource instanceof ClassPathResource);
        assertFalse(resource.exists());
        resource = Utils.resourceFromString("bad:doesNotExist", Set.of(CLASSPATH_PROTOCOL));
        assertTrue(resource instanceof ClassPathResource);
        assertFalse(resource.exists());

        // classpath is now not allowed either so it fails
        assertNotAllowed("URL [bad://invalid] can't be found or is not allowed for loading resources",
                () -> Utils.resourceFromString("bad://invalid", Set.of(FILE_PROTOCOL)));
    }

    // check urls that are allowed
    @Test
    public void testResourceFromStringUri2() throws Exception {
        Resource resource;

        // we should be able to build the resources now that they allowed even if they don't exist
        resource = Utils.resourceFromString("http://doesNotExist", Set.of("http"));
        assertTrue(resource instanceof UrlResource);
        assertFalse(resource.exists());

        resource = Utils.resourceFromString("jar:file:invalid.jar!/", Set.of("jar"));
        assertTrue(resource instanceof UrlResource);
        assertFalse(resource.exists());

        try {
            // allowed but should have malformed error
            Utils.resourceFromString("http:", Set.of("http"));
            fail("should have exception");
        } catch (MalformedURLException e) {
            assertTrue(e.getCause() instanceof URISyntaxException);
        }
    }

    private static void assertNotAllowed(String expected, Callable<?> callable) {
        try {
            callable.call();
            fail("Should have failed with Exception");
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
            assertEquals(expected, e.getMessage());
        }
    }
}
