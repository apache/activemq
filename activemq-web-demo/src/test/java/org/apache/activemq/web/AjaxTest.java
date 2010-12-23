package org.apache.activemq.web;


import java.util.Enumeration;
import javax.jms.TextMessage;
import javax.jms.MessageProducer;
import javax.management.ObjectName;

import org.apache.activemq.broker.jmx.DestinationViewMBean;
import org.apache.activemq.broker.jmx.SubscriptionViewMBean;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.httpclient.*;
import org.apache.commons.httpclient.methods.*;
import java.util.Set;

public class AjaxTest extends JettyTestSupport {
    private static final Log LOG = LogFactory.getLog(AjaxTest.class);

    private String expectedResponse = "<ajax-response>\n" +
            "<response id='handler' destination='queue://test' >test one</response>\n" +
            "<response id='handler' destination='queue://test' >test two</response>\n" +
            "<response id='handler' destination='queue://test' >test three</response>\n" +
            "</ajax-response>";

    public void testReceiveMultipleMessagesFromQueue() throws Exception {

        MessageProducer local_producer = session.createProducer(session.createQueue("test"));

        HttpClient httpClient = new HttpClient();
        PostMethod post = new PostMethod( "http://localhost:8080/amq" );
        post.addParameter( "destination", "queue://test" );
        post.addParameter( "type", "listen" );
        post.addParameter( "message", "handler" );
        httpClient.executeMethod( post );

        // send message
        TextMessage msg1 = session.createTextMessage("test one");
        producer.send(msg1);
        TextMessage msg2 = session.createTextMessage("test two");
        producer.send(msg2);
        TextMessage msg3 = session.createTextMessage("test three");
        producer.send(msg3);

        HttpMethod get = new GetMethod( "http://localhost:8080/amq?timeout=5000" );
        httpClient.executeMethod( get );
        byte[] responseBody = get.getResponseBody();
        String response = new String( responseBody );

        LOG.info("Poll response: " + response);
        assertEquals("Poll response not right", expectedResponse.trim(), response.trim());
    }

}
