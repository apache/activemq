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
package org.apache.activemq.plugin;

import static org.junit.Assert.assertEquals;

import java.io.StringReader;
import java.nio.file.Files;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilderFactory;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.xml.sax.InputSource;

public class PropertiesPlaceHolderUtilTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testPropertyPlaceholderConfigurerLocations() throws Exception {
        assertLocationsLoaded("org.springframework.beans.factory.config.PropertyPlaceholderConfigurer");
    }

    @Test
    public void testPropertySourcesPlaceholderConfigurerLocations() throws Exception {
        assertLocationsLoaded("org.springframework.context.support.PropertySourcesPlaceholderConfigurer");
    }

    @Test
    public void testEncryptableConfigurerLocations() throws Exception {
        assertLocationsLoaded("org.apache.activemq.spring.EncryptablePropertyPlaceholderConfigurer");
        assertLocationsLoaded("org.apache.activemq.spring.EncryptablePropertySourcesPlaceholderConfigurer");
    }

    private void assertLocationsLoaded(String configurerClass) throws Exception {
        var file = folder.newFile();
        Files.writeString(file.toPath(), "activemq.username=system\nactivemq.password=manager\n");

        var xml = "<beans xmlns=\"http://www.springframework.org/schema/beans\">"
            + "<bean class=\"" + configurerClass + "\">"
            + "<property name=\"localOverride\" value=\"true\"/>"
            + "<property name=\"locations\"><value>" + file.toURI() + "</value></property>"
            + "</bean></beans>";
        var factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        var document = factory.newDocumentBuilder().parse(new InputSource(new StringReader(xml)));

        var properties = new Properties();
        var util = new PropertiesPlaceHolderUtil(properties);
        util.mergeProperties(document, properties, null);

        assertEquals(configurerClass, "system", util.filter("${activemq.username}"));
        assertEquals(configurerClass, "manager", util.filter("${activemq.password}"));
    }
}
