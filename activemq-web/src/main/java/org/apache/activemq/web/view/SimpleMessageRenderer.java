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

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Enumeration;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.QueueBrowser;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * A simple rendering of the contents of a queue appear as a list of message
 * elements which just contain an ID attribute.
 * 
 * @version $Revision: $
 */
public class SimpleMessageRenderer implements MessageRenderer {

    private String contentType = "text/xml";
    private int maxMessages;

    public void renderMessages(HttpServletRequest request, HttpServletResponse response, QueueBrowser browser) throws IOException, JMSException, ServletException {
        // lets use XML by default
        response.setContentType(getContentType());
        PrintWriter writer = response.getWriter();
        printHeader(writer, browser, request);

        Enumeration iter = browser.getEnumeration();
        for (int counter = 0; iter.hasMoreElements() && (maxMessages <= 0 || counter < maxMessages); counter++) {
            Message message = (Message)iter.nextElement();
            renderMessage(writer, request, response, browser, message);
        }

        printFooter(writer, browser, request);
    }

    public void renderMessage(PrintWriter writer, HttpServletRequest request, HttpServletResponse response, QueueBrowser browser, Message message) throws JMSException, ServletException {
        // lets just write the message IDs for now
        writer.print("<message id='");
        writer.print(message.getJMSMessageID());
        writer.println("'/>");
    }

    // Properties
    // -------------------------------------------------------------------------
    public int getMaxMessages() {
        return maxMessages;
    }

    public void setMaxMessages(int maxMessages) {
        this.maxMessages = maxMessages;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    // Implementation methods
    // -------------------------------------------------------------------------

    protected void printHeader(PrintWriter writer, QueueBrowser browser, HttpServletRequest request) throws IOException, JMSException, ServletException {
        writer.println("");
        writer.print("<messages queue='");
        writer.print(browser.getQueue());
        writer.print("'");
        String selector = browser.getMessageSelector();
        if (selector != null) {
            writer.print(" selector='");
            writer.print(selector);
            writer.print("'");
        }
        writer.println(">");
    }

    protected void printFooter(PrintWriter writer, QueueBrowser browser, HttpServletRequest request) throws IOException, JMSException, ServletException {
        writer.println("</messages>");
    }

}
