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

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.FactoryConfigurationError;
import javax.xml.transform.TransformerFactory;

import org.apache.commons.xml.secure.SecureDocumentBuilderFactory;
import org.apache.commons.xml.secure.SecureTransformerFactory;

/**
 * Utility class to obtain XML-processing related factories with pre-configured safe parameters. Prefer to centralize
 * these parameters here instead of doing ad-hoc on several places.
 *
 * @deprecated Use {@link SecureDocumentBuilderFactory} and {@link SecureTransformerFactory}.
 */
@Deprecated
public final class XmlFactories {

    private XmlFactories() { /* Do not instantiate */ }

    /**
     * Gets a new, secure, namespace-aware {@link DocumentBuilderFactory}, enabling namespace awareness on {@link #newInstance()}, the behavior
     * {@code DocumentBuilderFactory.newNSInstance()} (Java 13 or later) is specified to have.
     *
     * @return A secure, namespace-aware factory.
     * @throws IllegalStateException     Thrown if a required secure setting cannot be applied to the underlying implementation.
     * @throws FactoryConfigurationError Thrown from a factory in case of a {@link java.util.ServiceConfigurationError service configuration error} or if the
     *                                   implementation is not available or cannot be instantiated.
     * @deprecated Use {@link SecureDocumentBuilderFactory#newNSInstance()} instead.
     */
    @Deprecated
    public static DocumentBuilderFactory getSafeDocumentBuilderFactory() {
        return SecureDocumentBuilderFactory.newNSInstance();
    }

    /**
     * Gets a new, secure {@link TransformerFactory}.
     *
     * @return A secure factory.
     * @throws IllegalStateException Thrown if a required secure setting cannot be applied to the underlying implementation.
     * @deprecated Use {@link SecureTransformerFactory#newInstance()} instead.
     */
    @Deprecated
    public static TransformerFactory getSafeTransformFactory() {
        return SecureTransformerFactory.newInstance();
    }

}
