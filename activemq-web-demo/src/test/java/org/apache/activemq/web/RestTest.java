package org.apache.activemq.web;

import javax.jms.TextMessage;

import org.eclipse.jetty.client.ContentExchange;
import org.eclipse.jetty.client.HttpClient;

public class RestTest extends JettyTestSupport {
	
	public void testConsume() throws Exception {
	    producer.send(session.createTextMessage("test"));
	    
	    HttpClient httpClient = new HttpClient();
        httpClient.start();
        ContentExchange contentExchange = new ContentExchange();
        httpClient.setConnectorType(HttpClient.CONNECTOR_SELECT_CHANNEL);
        contentExchange.setURL("http://localhost:8080/message/test?timeout=1000&type=queue");
        httpClient.send(contentExchange);
        contentExchange.waitForDone();
        assertEquals("test", contentExchange.getResponseContent());
	    
	}
	
	public void testSelector() throws Exception {
	    TextMessage msg1 = session.createTextMessage("test1");
	    msg1.setIntProperty("test", 1);
	    producer.send(msg1);
	    
	    TextMessage msg2 = session.createTextMessage("test2");
	    msg2.setIntProperty("test", 2);
	    producer.send(msg2);
	    
        HttpClient httpClient = new HttpClient();
        httpClient.start();
        ContentExchange contentExchange = new ContentExchange();
        httpClient.setConnectorType(HttpClient.CONNECTOR_SELECT_CHANNEL);
        contentExchange.setURL("http://localhost:8080/message/test?timeout=1000&type=queue");
        contentExchange.setRequestHeader("selector", "test=2");
        httpClient.send(contentExchange);
        contentExchange.waitForDone();
        assertEquals("test2", contentExchange.getResponseContent());
	}

}
