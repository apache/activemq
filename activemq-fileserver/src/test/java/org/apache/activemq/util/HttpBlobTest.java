package org.apache.activemq.util;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import junit.framework.Assert;

import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.BlobMessage;
import org.apache.activemq.command.ActiveMQBlobMessage;
import org.eclipse.jetty.util.IO;

public class HttpBlobTest extends HttpTestSupport {
    
    public void testBlobFile() throws Exception {
        // first create Message
        File file = File.createTempFile("amq-data-file-", ".dat");
        // lets write some data
        String content = "hello world " + System.currentTimeMillis();
        BufferedWriter writer = new BufferedWriter(new FileWriter(file));
        writer.append(content);
        writer.close();

        ActiveMQSession session = (ActiveMQSession) connection.createSession(
                false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(destination);
        MessageConsumer consumer = session.createConsumer(destination);
        BlobMessage message = session.createBlobMessage(file);

        producer.send(message);
        Thread.sleep(1000);

        // check message send
        Message msg = consumer.receive(1000);
        Assert.assertTrue(msg instanceof ActiveMQBlobMessage);

        InputStream input = ((ActiveMQBlobMessage) msg).getInputStream();
        StringBuilder b = new StringBuilder();
        int i = input.read();
        while (i != -1) {
            b.append((char) i);
            i = input.read();
        }
        input.close();
        File uploaded = new File(homeDir, msg.getJMSMessageID().toString().replace(":", "_")); 
        Assert.assertEquals(content, b.toString());
        assertTrue(uploaded.exists());
        ((ActiveMQBlobMessage)msg).deleteFile();
        assertFalse(uploaded.exists());
    }
    
}
