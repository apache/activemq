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
package org.apache.activemq.config;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Objects;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.junit.Test;
import org.xml.sax.EntityResolver;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;


public class ValidateXMLConfigTest {
    private static final String SCHEMA_LANGUAGE_ATTRIBUTE = "http://java.sun.com/xml/jaxp/properties/schemaLanguage";
    private static final String XSD_SCHEMA_LANGUAGE = "http://www.w3.org/2001/XMLSchema";

    
    @Test
    public void validateDefaultConfig() throws Exception {
        validateXML("src/release/conf/activemq.xml");
    }
    
    @Test
    public void validateExampleConfig() throws Exception {
        // resource:copy-resource brings all config files into target/conf
        File sampleConfDir = new File("target/conf");

        final HashSet<String> skipped = new HashSet<>(java.util.Arrays.asList("web.xml", "camel.xml", "jolokia-access.xml"));

        for (File xmlFile : Objects.requireNonNull(sampleConfDir.listFiles(pathname -> pathname.isFile() && pathname.getName().endsWith("xml") && !skipped.contains(pathname.getName())))) {
            validateXML(xmlFile);
        }
    }
    
    private void validateXML(String fileName) throws Exception {
        File xmlFile = new File(fileName);
        validateXML(xmlFile);
    }
        
    private void validateXML(File file) throws Exception {
        getDocumentBuilder(file.getAbsolutePath()).parse(file);
    }
    
    private DocumentBuilder getDocumentBuilder(final String fileName) throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setValidating(true);
        factory.setNamespaceAware(true);       
        factory.setAttribute(SCHEMA_LANGUAGE_ATTRIBUTE, XSD_SCHEMA_LANGUAGE);
        
        DocumentBuilder builder = factory.newDocumentBuilder();
       
        builder.setEntityResolver(new EntityResolver() {

            public InputSource resolveEntity(String publicId, String systemId)
                    throws SAXException, IOException {
                System.err.println("resolve: " + publicId + ", sys: " +  systemId);
                InputSource source = null;
                if (systemId.endsWith("activemq-core.xsd")) {
                   InputStream stream = this.getClass().getClassLoader().getResourceAsStream("activemq.xsd");
                   source = new InputSource(stream);
                   source.setPublicId(publicId);
                   source.setSystemId(systemId);
                }
                return source;       
            }
        });
        
        builder.setErrorHandler(new ErrorHandler() {
            public void error(SAXParseException exception) throws SAXException {
                fail(fileName + ", " + exception.toString());
            }
            public void fatalError(SAXParseException exception)
                    throws SAXException {
                fail(fileName + ", " + exception.toString());
            }
            public void warning(SAXParseException exception)
                    throws SAXException {
                fail(fileName + ", " + exception.toString());
            }
        });
        
        return builder;
    }
    
    
}
