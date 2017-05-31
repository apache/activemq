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

import org.apache.activemq.command.Message;
import org.apache.activemq.util.ByteArrayInputStream;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.TextMessage;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import java.io.StringReader;

public class XalanXPathEvaluator implements XPathExpression.XPathEvaluator {

    private static final XPathFactory FACTORY = XPathFactory.newInstance();
    private final String xpathExpression;
    private final DocumentBuilder builder;
    private final XPath xpath = FACTORY.newXPath();

    public XalanXPathEvaluator(String xpathExpression, DocumentBuilder builder) throws Exception {
        this.xpathExpression = xpathExpression;
        if (builder != null) {
            this.builder = builder;
        } else {
            throw new RuntimeException("No document builder available");
        }
    }

    public boolean evaluate(Message message) throws JMSException {
        if (message instanceof TextMessage) {
            String text = ((TextMessage)message).getText();
            return evaluate(text);
        } else if (message instanceof BytesMessage) {
            BytesMessage bm = (BytesMessage)message;
            byte data[] = new byte[(int)bm.getBodyLength()];
            bm.readBytes(data);
            return evaluate(data);
        }
        return false;
    }

    private boolean evaluate(byte[] data) {
        try {
            InputSource inputSource = new InputSource(new ByteArrayInputStream(data));
            Document inputDocument = builder.parse(inputSource);
            return ((Boolean) xpath.evaluate(xpathExpression, inputDocument, XPathConstants.BOOLEAN)).booleanValue();
        } catch (Exception e) {
            return false;
        }
    }

    private boolean evaluate(String text) {
        try {
            InputSource inputSource = new InputSource(new StringReader(text));
            Document inputDocument = builder.parse(inputSource);
            return ((Boolean) xpath.evaluate(xpathExpression, inputDocument, XPathConstants.BOOLEAN)).booleanValue();
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public String toString() {
        return xpathExpression;
    }
}
