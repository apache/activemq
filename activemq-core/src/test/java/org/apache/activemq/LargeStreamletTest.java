package org.apache.activemq;
/**
*
* Copyright 2005-2006 The Apache Software Foundation
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;

import javax.jms.Destination;
import javax.jms.Session;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author rnewson
 */
public final class LargeStreamletTest extends TestCase {

    private static final String BROKER_URL = "vm://localhost?broker.persistent=false";

    private static final int BUFFER_SIZE = 1 * 1024;

    private static final int MESSAGE_COUNT = 1024*1024;
    
    private int totalRead;

    private int totalWritten;

    private AtomicBoolean stopThreads = new AtomicBoolean(false);

    public void testStreamlets() throws Exception {
        final ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(
                BROKER_URL);

        final ActiveMQConnection connection = (ActiveMQConnection) factory
                .createConnection();
        connection.start();
        try {
            final Session session = connection.createSession(false,
                    Session.AUTO_ACKNOWLEDGE);
            try {
                final Destination destination = session.createQueue("wibble");
                final Thread readerThread = new Thread(new Runnable() {

                    public void run() {
                        totalRead = 0;
                        try {
                            final InputStream inputStream = connection
                                    .createInputStream(destination);
                            try {
                                int read;
                                final byte[] buf = new byte[BUFFER_SIZE];
                                while (!stopThreads.get()
                                        && (read = inputStream.read(buf)) != -1) {
                                    totalRead += read;
                                }
                            } finally {
                                inputStream.close();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        } finally {
                            System.err
                                    .println(totalRead + " total bytes read.");
                        }
                    }
                });

                final Thread writerThread = new Thread(new Runnable() {

                    public void run() {
                        totalWritten = 0;
                        int count = MESSAGE_COUNT;
                        try {
                            final OutputStream outputStream = connection
                                    .createOutputStream(destination);
                            try {
                                final byte[] buf = new byte[BUFFER_SIZE];
                                new Random().nextBytes(buf);
                                while (count > 0 && !stopThreads.get()) {
                                    outputStream.write(buf);
                                    totalWritten += buf.length;
                                    count--;
                                }
                            } finally {
                                outputStream.close();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        } finally {
                            System.err.println(totalWritten
                                    + " total bytes written.");
                        }
                    }
                });

                readerThread.start();
                writerThread.start();

                readerThread.join(30*1000);
                writerThread.join(10);
                
                stopThreads.set(true);
                                
                Assert.assertEquals("Not all messages accounted for", 
                        totalWritten, totalRead);
                
            } finally {
                session.close();
            }
        } finally {
            connection.close();
        }
    }

}
