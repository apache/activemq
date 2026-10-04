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

import org.apache.commons.xml.secure.SecureDocumentBuilderFactory;
import org.apache.commons.xml.secure.SecureTransformerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerFactory;

/**
 * Utility class to obtain XML-processing related factories with pre-configured safe parameters. Prefer to centralize
 * these parameters here instead of doing ad-hoc on several places.
 */
public final class XmlFactories {
    private static final Logger LOG = LoggerFactory.getLogger(XmlFactories.class);

    private XmlFactories() { /* Do not instantiate */ }

    public static DocumentBuilderFactory getSafeDocumentBuilderFactory() {
        DocumentBuilderFactory builderFactory = SecureDocumentBuilderFactory.newInstance();

        // See https://github.com/OWASP/CheatSheetSeries/blob/master/cheatsheets/XML_External_Entity_Prevention_Cheat_Sheet.md#java
        trySetFeature(builderFactory,"http://apache.org/xml/features/disallow-doctype-decl", true);

        return builderFactory;
    }

    public static TransformerFactory getSafeTransformFactory() {
        return SecureTransformerFactory.newInstance();
    }

    private static void trySetFeature(final DocumentBuilderFactory factory, final String name, final boolean value) {
        try {
            factory.setFeature(name, value);
        } catch (final ParserConfigurationException e) {
            LOG.warn("Error setting document builder factory feature", e);
        }
    }

}
