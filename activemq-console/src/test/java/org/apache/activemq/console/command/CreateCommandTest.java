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
package org.apache.activemq.console.command;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import org.apache.activemq.console.CommandContext;
import org.apache.activemq.console.formatter.CommandShellOutputFormatter;
import org.apache.commons.xml.secure.SecureDocumentBuilderFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class CreateCommandTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private void assertCopiesConfiguration(String beansNamespace, String brokerNamespace) throws Exception {
        File sourceBase = temporaryFolder.newFolder("source");
        File targetBase = temporaryFolder.newFolder("target");
        Files.createDirectories(new File(targetBase, "conf").toPath());
        File source = new File(sourceBase, "custom.xml");
        String configuration = "<beans xmlns=\"" + beansNamespace + "\"><broker xmlns=\"" + brokerNamespace
                + "\" brokerName=\"original\" persistent=\"false\"><transportConnectors>"
                + "<transportConnector name=\"openwire\" uri=\"tcp://localhost:61616\"/>"
                + "</transportConnectors></broker></beans>";
        Files.write(source.toPath(), configuration.getBytes(StandardCharsets.UTF_8));

        CommandContext context = new CommandContext();
        context.setFormatter(new CommandShellOutputFormatter(new ByteArrayOutputStream()));
        CreateCommand command = new CreateCommand();
        command.setCommandContext(context);
        command.brokerName = "copied-broker";
        command.copyActivemqConf(sourceBase, targetBase, "custom.xml");

        File destination = new File(targetBase, "conf/activemq.xml");
        assertTrue("Copied configuration exists", destination.isFile());
        Document copied = SecureDocumentBuilderFactory.newNSInstance().newDocumentBuilder().parse(destination);
        assertEquals("beans", copied.getDocumentElement().getLocalName());
        Element broker = (Element) copied.getElementsByTagNameNS("*", "broker").item(0);
        assertEquals("copied-broker", broker.getAttribute("brokerName"));
        assertEquals("false", broker.getAttribute("persistent"));
        assertEquals(brokerNamespace, broker.getNamespaceURI() == null ? "" : broker.getNamespaceURI());
        Element connector = (Element) broker.getElementsByTagNameNS("*", "transportConnector").item(0);
        assertEquals("openwire", connector.getAttribute("name"));
        assertEquals("tcp://localhost:61616", connector.getAttribute("uri"));
        assertEquals("Source configuration is unchanged", configuration, Files.readString(source.toPath(), StandardCharsets.UTF_8));
    }

    @Test
    public void testCopyActivemqConfWithNamespaces() throws Exception {
        assertCopiesConfiguration("http://www.springframework.org/schema/beans", "http://activemq.apache.org/schema/core");
    }

    @Test
    public void testCopyActivemqConfWithoutNamespaces() throws Exception {
        assertCopiesConfiguration("", "");
    }
}
