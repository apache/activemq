/*
 * Created on Feb 21, 2005
 *
 * To change the template for this generated file go to
 * Window - Preferences - Java - Code Generation - Code and Comments
 */

package org.activemq.streams;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import javax.jms.Destination;

import junit.framework.Test;

import org.activemq.ActiveMQConnection;
import org.activemq.JmsTestSupport;
import org.activemq.command.ActiveMQQueue;
import org.activemq.command.ActiveMQTopic;

import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicBoolean;

/**
 * JMSInputStreamTest
 */
public class JMSInputStreamTest extends JmsTestSupport {
    
    protected DataOutputStream out;
    protected DataInputStream in;
    private ActiveMQConnection connection2;

    public Destination destination;
    
    public static Test suite() {
        return suite(JMSInputStreamTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
    
    public void initCombos() {
        addCombinationValues("destination", new Object[] { 
                new ActiveMQQueue("TEST.QUEUE"),
                new ActiveMQTopic("TEST.TOPIC") });
    }

    /*
     * @see TestCase#setUp()
     */
    protected void setUp() throws Exception {
        super.setUp();
        connection2 = (ActiveMQConnection) factory.createConnection(userName, password);
        connections.add(connection2);
        out = new DataOutputStream(connection.createOutputStream(destination));
        in = new DataInputStream(connection2.createInputStream(destination));
    }

    /*
     * @see TestCase#tearDown()
     */
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testStreams() throws Exception {
        out.writeInt(4);
        out.flush();
        assertTrue(in.readInt() == 4);
        out.writeFloat(2.3f);
        out.flush();
        assertTrue(in.readFloat() == 2.3f);
        String str = "this is a test string";
        out.writeUTF(str);
        out.flush();
        assertTrue(in.readUTF().equals(str));
        for (int i = 0;i < 100;i++) {
            out.writeLong(i);
        }
        out.flush();
        for (int i = 0;i < 100;i++) {
            assertTrue(in.readLong() == i);
        }
    }

    public void testLarge() throws Exception {
        final int TEST_DATA = 23;
        final int DATA_LENGTH = 4096;
        final int COUNT = 1024;
        byte[] data = new byte[DATA_LENGTH];
        for (int i = 0;i < data.length;i++) {
            data[i] = TEST_DATA;
        }
        final AtomicBoolean complete = new AtomicBoolean(false);
        Thread runner = new Thread(new Runnable() {
            public void run() {
                try {
                    for (int x = 0;x < COUNT;x++) {
                        byte[] b = new byte[2048];
                        in.readFully(b);
                        for (int i = 0;i < b.length;i++) {
                            assertTrue(b[i] == TEST_DATA);
                        }
                    }
                    complete.set(true);
                    synchronized(complete){
                        complete.notify();
                    }
                }
                catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        });
        runner.start();
        for (int i = 0;i < COUNT;i++) {
            out.write(data);
        }
        out.flush();
        synchronized (complete) {
            if (!complete.get()) {
                complete.wait(30000);
            }
        }
        assertTrue(complete.get());
    }
}