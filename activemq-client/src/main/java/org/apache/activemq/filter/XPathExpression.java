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
package org.apache.activemq.filter;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.jms.JMSException;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.activemq.command.Message;
import org.apache.activemq.util.JMSExceptionSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used to evaluate an XPath Expression in a JMS selector.
 */
public final class XPathExpression implements BooleanExpression {

    private static final Logger LOG = LoggerFactory.getLogger(XPathExpression.class);
    private static final String EVALUATOR_SYSTEM_PROPERTY = "org.apache.activemq.XPathEvaluatorClassName";
    private static final String DEFAULT_EVALUATOR_CLASS_NAME = "org.apache.activemq.filter.XalanXPathEvaluator";
    public static final String DOCUMENT_BUILDER_FACTORY_FEATURE = "org.apache.activemq.documentBuilderFactory.feature";

    private static final Constructor EVALUATOR_CONSTRUCTOR;
    private static DocumentBuilder builder = null;

    static {
        String cn = System.getProperty(EVALUATOR_SYSTEM_PROPERTY, DEFAULT_EVALUATOR_CLASS_NAME);
        Constructor m = null;
        try {
            try {
                m = getXPathEvaluatorConstructor(cn);
                DocumentBuilderFactory builderFactory = DocumentBuilderFactory.newInstance();
                builderFactory.setNamespaceAware(true);
                builderFactory.setIgnoringElementContentWhitespace(true);
                builderFactory.setIgnoringComments(true);
                try {
                    // set some reasonable defaults
                    builderFactory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, Boolean.TRUE);
                    builderFactory.setFeature("http://xml.org/sax/features/external-general-entities", false);
                    builderFactory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
                    builderFactory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
                } catch (ParserConfigurationException e) {
                    LOG.warn("Error setting document builder factory feature", e);
                }
                // setup the feature from the system property
                setupFeatures(builderFactory);
                builder = builderFactory.newDocumentBuilder();
            } catch (Throwable e) {
                LOG.warn("Invalid " + XPathEvaluator.class.getName() + " implementation: " + cn + ", reason: " + e, e);
                cn = DEFAULT_EVALUATOR_CLASS_NAME;
                try {
                    m = getXPathEvaluatorConstructor(cn);
                } catch (Throwable e2) {
                    LOG.error("Default XPath evaluator could not be loaded", e);
                }
            }
        } finally {
            EVALUATOR_CONSTRUCTOR = m;
        }
    }

    private final String xpath;
    private final XPathEvaluator evaluator;

    public static interface XPathEvaluator {
        boolean evaluate(Message message) throws JMSException;
    }

    XPathExpression(String xpath) {
        this.xpath = xpath;
        this.evaluator = createEvaluator(xpath);
    }

    private static Constructor getXPathEvaluatorConstructor(String cn) throws ClassNotFoundException, SecurityException, NoSuchMethodException {
        Class c = XPathExpression.class.getClassLoader().loadClass(cn);
        if (!XPathEvaluator.class.isAssignableFrom(c)) {
            throw new ClassCastException("" + c + " is not an instance of " + XPathEvaluator.class);
        }
        return c.getConstructor(new Class[] {String.class, DocumentBuilder.class});
    }

    protected static void setupFeatures(DocumentBuilderFactory factory) {
        Properties properties = System.getProperties();
        List<String> features = new ArrayList<String>();
        for (Map.Entry<Object, Object> prop : properties.entrySet()) {
            String key = (String) prop.getKey();
            if (key.startsWith(DOCUMENT_BUILDER_FACTORY_FEATURE)) {
                String uri = key.split(DOCUMENT_BUILDER_FACTORY_FEATURE + ":")[1];
                Boolean value = Boolean.valueOf((String)prop.getValue());
                try {
                    factory.setFeature(uri, value);
                    features.add("feature " + uri + " value " + value);
                } catch (ParserConfigurationException e) {
                    LOG.warn("DocumentBuilderFactory doesn't support the feature {} with value {}, due to {}.", new Object[]{uri, value, e});
                }
            }
        }
        if (features.size() > 0) {
            StringBuffer featureString = new StringBuffer();
            // just log the configured feature
            for (String feature : features) {
                if (featureString.length() != 0) {
                    featureString.append(", ");
                }
                featureString.append(feature);
            }
        }

    }

    private XPathEvaluator createEvaluator(String xpath2) {
        try {
            return (XPathEvaluator)EVALUATOR_CONSTRUCTOR.newInstance(new Object[] {xpath, builder});
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException)cause;
            }
            throw new RuntimeException("Invalid XPath Expression: " + xpath + " reason: " + e.getMessage(), e);
        } catch (Throwable e) {
            throw new RuntimeException("Invalid XPath Expression: " + xpath + " reason: " + e.getMessage(), e);
        }
    }

    public Object evaluate(MessageEvaluationContext message) throws JMSException {
        try {
            if (message.isDropped()) {
                return null;
            }
            return evaluator.evaluate(message.getMessage()) ? Boolean.TRUE : Boolean.FALSE;
        } catch (IOException e) {
            throw JMSExceptionSupport.create(e);
        }

    }

    public String toString() {
        return "XPATH " + ConstantExpression.encodeString(xpath);
    }

    /**
     * @param message
     * @return true if the expression evaluates to Boolean.TRUE.
     * @throws JMSException
     */
    public boolean matches(MessageEvaluationContext message) throws JMSException {
        Object object = evaluate(message);
        return object != null && object == Boolean.TRUE;
    }

}
