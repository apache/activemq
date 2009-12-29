package org.apache.activemq.transport.stomp;

import java.net.Socket;
import java.net.URI;
import java.util.HashMap;

import junit.framework.TestCase;

import org.apache.activemq.transport.stomp.StompConnection;
import org.apache.activemq.transport.stomp.StompFrame;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * Simulates load on the Stomp connector. All producers/consumers open/close a
 * connection on every command Configurable number of producers/consumers, their
 * speed and duration of test
 * 
 * Start a broker with the desired configuration to test and then run this test
 * 
 */
public class StompLoadTest extends TestCase {

    private static final Log LOG = LogFactory.getLog(StompLoadTest.class);

    final int producerSleep = 10;
    final int consumerSleep = 10;
    final int msgCount = 10000;
    final int producerCount = 5;
    final int consumerCount = 5;
    final int testTime = 30 * 60 * 1000;
    final String bindAddress = "stomp://0.0.0.0:61612";

    public void testLoad() throws Exception {

        for (int i = 0; i < producerCount; i++) {
            ProducerThread producerThread = new ProducerThread("producer" + i);
            producerThread.start();
        }

        for (int i = 0; i < consumerCount; i++) {
            Thread consumerThread = new ConsumerThread("consumer" + i);
            consumerThread.start();
        }

        Thread.sleep(testTime);
    }

    public StompConnection createConnection() throws Exception {
        StompConnection conn = new StompConnection();
        URI connectUri = new URI(bindAddress);
        conn.open(new Socket(connectUri.getHost(), connectUri.getPort()));
        conn.connect("", "");
        return conn;
    }

    class ProducerThread extends Thread {

        String name;

        public ProducerThread(String name) {
            this.name = name;
        }

        public void run() {
            for (int i = 0; i < msgCount; i++) {
                try {
                    StompConnection conn = createConnection();
                    String msg = "test message " + i;
                    LOG.info(name + " sending " + msg);
                    conn.send("/queue/test", msg);
                    conn.disconnect();
                    Thread.sleep(producerSleep);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    class ConsumerThread extends Thread {

        String name;

        public ConsumerThread(String name) {
            this.name = name;
        }

        public void run() {
            for (int i = 0; i < msgCount; i++) {
                try {
                    StompConnection conn = createConnection();
                    HashMap<String, String> headers = new HashMap<String, String>();
                    headers.put("activemq.prefetchSize", "1");
                    conn.subscribe("/queue/test", "client", headers);
                    StompFrame frame = conn.receive(1000);
                    conn.ack(frame);
                    LOG.info(name + " received " + frame.getBody());
                    conn.disconnect();
                    Thread.sleep(consumerSleep);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
