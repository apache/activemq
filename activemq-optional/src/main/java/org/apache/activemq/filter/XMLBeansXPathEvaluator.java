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

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.TextMessage;

import org.apache.activemq.command.Message;
import org.apache.activemq.util.ByteArrayInputStream;
import org.apache.xmlbeans.XmlObject;

public class XMLBeansXPathEvaluator implements XPathExpression.XPathEvaluator {

    private final String xpath;

    public XMLBeansXPathEvaluator(String xpath) {
        this.xpath = xpath;
    }

    public boolean evaluate(Message message) throws JMSException {
        if (message instanceof TextMessage) {
            String text = ((TextMessage)message).getText();
            try {
                XmlObject object = XmlObject.Factory.parse(text);
                XmlObject[] objects = object.selectPath(xpath);
                return object != null && objects.length > 0;
            } catch (Throwable e) {
                return false;
            }

        } else if (message instanceof BytesMessage) {
            BytesMessage bm = (BytesMessage)message;
            byte data[] = new byte[(int)bm.getBodyLength()];
            bm.readBytes(data);
            try {
                XmlObject object = XmlObject.Factory.parse(new ByteArrayInputStream(data));
                XmlObject[] objects = object.selectPath(xpath);
                return object != null && objects.length > 0;
            } catch (Throwable e) {
                return false;
            }
        }
        return false;
    }
}
