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
package org.apache.activemq.maven;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Helper to convert relative paths to XBean description files to URL-compliant absolute paths.
 * 
 * @author Marc CARRE <carre.marc@gmail.com>
 */
public class XBeanFileResolver {
    private static final String XBEAN_FILE = "xbean:file:";

    /**
     * Check if the provided path is an URL to a XBean file (xbean:file:<path/to/file>)
     */
    public boolean isXBeanFile(final String configUri) {
        return configUri.startsWith(XBEAN_FILE);
    }

    /**
     * Convert provided path into a URL-style absolute path. See also:
     * http://maven.apache.org/plugin-developers/common-bugs.html# Converting_between_URLs_and_Filesystem_Paths
     */
    public String toUrlCompliantAbsolutePath(final String configUri) {
        if (!isXBeanFile(configUri))
            return configUri;

        String filePath = extractFilePath(configUri);
        return XBEAN_FILE + toAbsolutePath(filePath);
    }

    private String extractFilePath(final String configUri) {
        return configUri.substring(getIndexFilePath(configUri), configUri.length());
    }

    private int getIndexFilePath(final String configUri) {
        return configUri.indexOf(XBEAN_FILE) + XBEAN_FILE.length();
    }

    private String toAbsolutePath(final String path) {
        try {
            final URL url = new File(path).toURI().toURL();
            return toFilePath(url);
        } catch (MalformedURLException e) {
            throw new RuntimeException("Failed to resolve relative path for: " + path, e);
        }
    }

    private String toFilePath(final URL url) {
        String filePath = url.getFile();
        return underWindows() ? removePrependingSlash(filePath) : filePath;
    }

    private String removePrependingSlash(String filePath) {
        // Remove prepending '/' because path would be /C:/temp/file.txt, as URL would be file:/C:/temp/file.txt
        return filePath.substring(1, filePath.length());
    }

    private boolean underWindows() {
        String os = System.getProperty("os.name").toLowerCase();
        return (os.indexOf("win") >= 0);
    }
}
