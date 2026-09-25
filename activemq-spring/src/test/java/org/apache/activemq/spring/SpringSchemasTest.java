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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.util.Properties;

import org.junit.Test;

/**
 * Verifies that META-INF/spring.schemas contains a mapping for the version
 * being built. The versioned entry is generated from the project version by
 * resource filtering, so releases do not require a manual edit of the file.
 */
public class SpringSchemasTest {

    private static final String SCHEMA_BASE = "http://activemq.apache.org/schema/core";

    @Test
    public void testCurrentVersionIsMapped() throws Exception {
        var schemas = load("/META-INF/spring.schemas");
        var version = load("/spring-schemas-test.properties").getProperty("project.version");

        assertNotNull("project.version not resolved in test resource", version);
        assertFalse("project.version was not filtered", version.contains("${"));

        assertEquals("activemq.xsd", schemas.getProperty(SCHEMA_BASE + "/activemq-core-" + version + ".xsd"));
        assertEquals("activemq.xsd", schemas.getProperty(SCHEMA_BASE + "/activemq-core.xsd"));
        assertEquals("activemq.xsd", schemas.getProperty(SCHEMA_BASE));
    }

    @Test
    public void testNoUnresolvedPlaceholders() throws Exception {
        var schemas = load("/META-INF/spring.schemas");
        for (var name : schemas.stringPropertyNames()) {
            assertFalse("unresolved placeholder in schema mapping: " + name, name.contains("${"));
        }
    }

    private Properties load(String resource) throws Exception {
        var properties = new Properties();
        try (var in = getClass().getResourceAsStream(resource)) {
            assertNotNull("resource not found: " + resource, in);
            properties.load(in);
        }
        return properties;
    }
}
