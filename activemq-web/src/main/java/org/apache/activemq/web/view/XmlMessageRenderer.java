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
package org.apache.activemq.web.view;

import java.io.PrintWriter;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.QueueBrowser;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.thoughtworks.xstream.XStream;

/**
 * This renderer uses XStream to render messages on a queue as full XML elements
 * 
 * 
 */
public class XmlMessageRenderer extends SimpleMessageRenderer {

    private XStream xstream;
    
    public void renderMessage(PrintWriter writer, HttpServletRequest request, HttpServletResponse response, QueueBrowser browser, Message message) throws JMSException {
        getXstream().toXML(message, writer);
    }

    public XStream getXstream() {
        if (xstream == null) {
            xstream = new XStream();
            XStream.setupDefaultSecurity(xstream);
        }
        return xstream;
    }

    public void setXstream(XStream xstream) {
        this.xstream = xstream;
    }
}
