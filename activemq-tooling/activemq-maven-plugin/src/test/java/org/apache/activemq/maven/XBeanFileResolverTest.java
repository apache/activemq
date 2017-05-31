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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

/**
 * Test for: Helper to convert relative paths to XBean description files to URL-compliant absolute paths.
 */
public class XBeanFileResolverTest {
    private static final String XBEAN_FILE = "xbean:file:";

    @Test
    public void urlToXBeanFileShouldBeResolvedToAbsolutePath() throws IOException {
        XBeanFileResolver xBeanFileResolver = new XBeanFileResolver();

        String currentDirectory = getCurrentDirectoryLinuxStyle();
        String relativeXBeanFilePath = "src/main/resources/activemq.xml";

        // e.g. xbean:file:C:/dev/src/active-mq/activemq-tooling/activemq-maven-plugin/src/main/resources/activemq.xml
        String expectedUrl = XBEAN_FILE + currentDirectory + "/" + relativeXBeanFilePath;

        String actualUrl = xBeanFileResolver.toUrlCompliantAbsolutePath(XBEAN_FILE + relativeXBeanFilePath);

        assertEquals(expectedUrl, actualUrl);
    }

    private String getCurrentDirectoryLinuxStyle() throws IOException {
        String currentDirectory = new File(".").getCanonicalPath();
        return currentDirectory.replace("\\", "/");
    }
}
