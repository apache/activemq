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
package org.apache.activemq.maven.xbean;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.xbean.spring.generator.ElementMapping;
import org.apache.xbean.spring.generator.LogFacade;
import org.apache.xbean.spring.generator.NamespaceMapping;

/** Shared access to the sample source trees under src/test/resources/samples. */
final class SampleSupport {

    static final String NAMESPACE = "http://activemq.apache.org/schema/sample";
    static final File SAMPLES = new File("src/test/resources/samples");

    static final LogFacade QUIET_LOG = new LogFacade() {
        @Override
        public void log(String message) {
        }

        @Override
        public void log(String message, int level) {
        }
    };

    private SampleSupport() {
    }

    static File sample(String name) {
        File dir = new File(SAMPLES, name);
        assertTrue("sample directory missing: " + dir.getAbsolutePath(), dir.isDirectory());
        return dir;
    }

    static NamespaceMapping load(String sampleDir, Set<String> excludedClasses) throws IOException {
        var loader = new JavacMappingLoader(NAMESPACE, List.of(sample(sampleDir)), excludedClasses, List.of(), true, m -> { }, m -> { });
        Set<NamespaceMapping> namespaces = loader.loadNamespaces();
        assertTrue("expected exactly one namespace, got " + namespaces.size(), namespaces.size() == 1);
        return namespaces.iterator().next();
    }

    static ElementMapping element(NamespaceMapping namespace, String name) {
        ElementMapping element = namespace.getElement(name);
        assertNotNull("no element named " + name + " among " + namespace.getElements(), element);
        return element;
    }
}
