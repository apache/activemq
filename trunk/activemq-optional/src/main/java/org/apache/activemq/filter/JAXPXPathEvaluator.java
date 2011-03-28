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

import java.io.StringReader;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.TextMessage;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.xml.sax.InputSource;

import org.apache.activemq.command.Message;
import org.apache.activemq.util.ByteArrayInputStream;

public class JAXPXPathEvaluator implements XPathExpression.XPathEvaluator {

    private static final XPathFactory FACTORY = XPathFactory.newInstance();
    private javax.xml.xpath.XPathExpression expression;

    public JAXPXPathEvaluator(String xpathExpression) {
        try {
            XPath xpath = FACTORY.newXPath();
            expression = xpath.compile(xpathExpression);
        } catch (XPathExpressionException e) {
            throw new RuntimeException("Invalid XPath expression: " + xpathExpression);
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
            return ((Boolean)expression.evaluate(inputSource, XPathConstants.BOOLEAN)).booleanValue();
        } catch (XPathExpressionException e) {
            return false;
        }
    }

    private boolean evaluate(String text) {
        try {
            InputSource inputSource = new InputSource(new StringReader(text));
            return ((Boolean)expression.evaluate(inputSource, XPathConstants.BOOLEAN)).booleanValue();
        } catch (XPathExpressionException e) {
            return false;
        }
    }
}
