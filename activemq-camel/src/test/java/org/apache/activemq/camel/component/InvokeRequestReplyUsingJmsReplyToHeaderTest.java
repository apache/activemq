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
package org.apache.activemq.camel.component;

import static org.apache.activemq.camel.component.ActiveMQComponent.activeMQComponent;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.Destination;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.camel.CamelContext;
import org.apache.camel.ContextTestSupport;
import org.apache.camel.Exchange;
import org.apache.camel.Headers;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.jms.JmsConstants;
import org.apache.camel.component.mock.AssertionClause;
import org.apache.camel.component.mock.MockEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @version $Revision$
 */
public class InvokeRequestReplyUsingJmsReplyToHeaderTest extends ContextTestSupport {
    private static final transient Logger LOG = LoggerFactory.getLogger(ActiveMQReplyToHeaderUsingConverterTest.class);
    protected String replyQueueName = "queue://test.reply";
    protected Object correlationID = "ABC-123";
    protected Object groupID = "GROUP-XYZ";
    private MyServer myBean = new MyServer();

    public void testPerformRequestReplyOverJms() throws Exception {
        MockEndpoint resultEndpoint = getMockEndpoint("mock:result");

        resultEndpoint.expectedBodiesReceived("Hello James");
        AssertionClause firstMessage = resultEndpoint.message(0);
        firstMessage.header("JMSCorrelationID").isEqualTo(correlationID);
/*
        TODO - allow JMS headers to be copied?

        firstMessage.header("cheese").isEqualTo(123);
        firstMessage.header("JMSXGroupID").isEqualTo(groupID);
        firstMessage.header("JMSReplyTo").isEqualTo(ActiveMQConverter.toDestination(replyQueueName));
*/
        
        Map<String, Object> headers = new HashMap<String, Object>();
        headers.put("cheese", 123);
        headers.put("JMSReplyTo", replyQueueName);
        headers.put("JMSCorrelationID", correlationID);
        headers.put("JMSXGroupID", groupID);
        
        
        // Camel 2.0 ignores JMSReplyTo, so we're using replyTo MEP property
        template.request("activemq:test.server?replyTo=queue:test.reply", new Processor() {
            public void process(Exchange exchange) {
                exchange.getIn().setBody("James");
                Map<String, Object> headers = new HashMap<String, Object>();
                headers.put("cheese", 123);
                headers.put("JMSReplyTo", replyQueueName);
                headers.put("JMSCorrelationID", correlationID);
                headers.put("JMSXGroupID", groupID);
                exchange.getIn().setHeaders(headers);
            }
        });

        resultEndpoint.assertIsSatisfied();

        List<Exchange> list = resultEndpoint.getReceivedExchanges();
        Exchange exchange = list.get(0);
        Message in = exchange.getIn();
        Object replyTo = in.getHeader("JMSReplyTo");
        LOG.info("Reply to is: " + replyTo);

        LOG.info("Received headers: " + in.getHeaders());
        LOG.info("Received body: " + in.getBody());

        assertMessageHeader(in, "JMSCorrelationID", correlationID);

        /*
        TODO
        Destination destination = assertIsInstanceOf(Destination.class, replyTo);
        assertEquals("ReplyTo", replyQueueName, destination.toString());
        assertMessageHeader(in, "cheese", 123);
        assertMessageHeader(in, "JMSXGroupID", groupID);
        */

        Map<String,Object> receivedHeaders = myBean.getHeaders();

        assertThat(receivedHeaders, hasKey("JMSReplyTo"));
        assertThat(receivedHeaders, hasEntry("JMSXGroupID", groupID));
        assertThat(receivedHeaders, hasEntry("JMSCorrelationID", correlationID));

        replyTo = receivedHeaders.get("JMSReplyTo");
        LOG.info("Reply to is: " + replyTo);
        Destination destination = assertIsInstanceOf(Destination.class, replyTo);
        assertEquals("ReplyTo", replyQueueName, destination.toString());

        
    }

    protected CamelContext createCamelContext() throws Exception {
        CamelContext camelContext = super.createCamelContext();

        // START SNIPPET: example
        camelContext.addComponent("activemq", activeMQComponent("vm://localhost?broker.persistent=false"));
        // END SNIPPET: example

        return camelContext;
    }

    protected RouteBuilder createRouteBuilder() throws Exception {
        return new RouteBuilder() {
            public void configure() throws Exception {
                from("activemq:test.server").bean(myBean);

                from("activemq:test.reply").to("mock:result");
            }
        };
    }

    protected static class MyServer {
        private Map<String,Object> headers;

        public String process(@Headers Map<String,Object> headers, String body) {
            this.headers = headers;
            LOG.info("process() invoked with headers: " + headers);
            return "Hello " + body;
        }

        public Map<String,Object> getHeaders() {
            return headers;
        }
    }
}
