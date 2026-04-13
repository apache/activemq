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
package org.apache.activemq.spring;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Set;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.util.ResourceUtils;

public class Utils {

    public static final String FILE_PROTOCOL = "file";
    public static final String CLASSPATH_PROTOCOL = "classpath";

    public static Resource resourceFromString(String uri) throws MalformedURLException {
        // default allows all
        return resourceFromString(uri, null);
    }

    public static Resource resourceFromString(String uri, Set<String> allowedProtocols) throws MalformedURLException {
        // Empty set means nothing is allowed
        if (allowedProtocols != null && allowedProtocols.isEmpty()) {
            throw new IllegalArgumentException("No protocols are allowed for loading resources.");
        }

        final Resource resource;

        // First, just try and load a local file (if it exists) and if "file"
        // as part of the allow list. This preserves previous behavior of
        // always optimistically trying a local file first.
        if (isAllowFile(allowedProtocols) && new File(uri).exists()) {
            resource = new FileSystemResource(uri);
        // If file isn't allowed, or if the file can't be found then check
        // if the string is a valid URL. If it's valid, then we need
        // to validate if it's allowed before loading the URL.
        // isUrl() uses URI internally so it won't actually load anything
        } else if (ResourceUtils.isUrl(uri)) {
            try {
                validateUrlAllowed(uri, allowedProtocols);
                resource = new UrlResource(ResourceUtils.getURL(uri));
            } catch (FileNotFoundException | URISyntaxException e) {
                MalformedURLException malformedURLException = new MalformedURLException(uri);
                malformedURLException.initCause(e);
                throw malformedURLException;
            }
        // Fallback to trying on the classpath if not a valid Url, and we allow it which
        // also preserves the previous behavior (if classpath is allowed)
        } else if (isAllowClasspath(allowedProtocols)){
            resource = new ClassPathResource(uri);
        // Catch all fail-safe if nothing else matches. This could happen if file is allowed
        // but not classpath but the file doesn't exist
        } else {
            throw new IllegalArgumentException("URL [" + uri + "] can't be found or is not allowed"
                    + " for loading resources");
        }
        return resource;
    }

    static boolean isAllowFile(Set<String> allowedProtocols) {
        return allowedProtocols == null || allowedProtocols.contains(FILE_PROTOCOL);
    }

    static boolean isAllowClasspath(Set<String> allowedProtocols) {
        return allowedProtocols == null || allowedProtocols.contains(CLASSPATH_PROTOCOL);
    }

    private static void validateUrlAllowed(String uriString, Set<String> allowedProtocols)
            throws URISyntaxException {
        // Use new URI() to get the scheme
        // This is important because ResourceUtils.getURL() actually searches
        // the classpath which we don't want to do if not allowed
        if (allowedProtocols != null && !allowedProtocols.contains(new URI(uriString).getScheme())) {
            throw new IllegalArgumentException("URL [" + uriString +
                    "] does not use an allowed protocol for loading URL resources");
        }
    }
}
