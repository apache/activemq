/**
 * 
 * Copyright 2004 Hiram Chirino
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
 * 
 **/
package org.activeio.oneport;

import edu.emory.mathcs.backport.java.util.concurrent.ArrayBlockingQueue;
import edu.emory.mathcs.backport.java.util.concurrent.BlockingQueue;
import edu.emory.mathcs.backport.java.util.concurrent.ScheduledThreadPoolExecutor;
import edu.emory.mathcs.backport.java.util.concurrent.ThreadFactory;
import edu.emory.mathcs.backport.java.util.concurrent.ThreadPoolExecutor;
import edu.emory.mathcs.backport.java.util.concurrent.TimeUnit;
import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicInteger;

import org.activeio.AcceptListener;
import org.activeio.AsyncChannel;
import org.activeio.AsyncChannelFactory;
import org.activeio.AsyncChannelServer;
import org.activeio.Channel;
import org.activeio.FilterAsyncChannelServer;
import org.activeio.FilterSyncChannel;
import org.activeio.Packet;
import org.activeio.SyncChannel;
import org.activeio.adapter.AsyncToSyncChannel;
import org.activeio.adapter.SyncToAsyncChannelFactory;
import org.activeio.net.SocketMetadata;
import org.activeio.net.SocketSyncChannelFactory;
import org.activeio.packet.ByteArrayPacket;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.naming.NamingException;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import junit.framework.TestCase;

/**
 */
public class OnePortAsyncChannelServerTest extends TestCase {

    static private Log log = LogFactory.getLog(OnePortAsyncChannelServerTest.class);
    static public AtomicInteger serverPacketCounter = new AtomicInteger(0);

    public OnePortAsyncChannelServer server;
    public AsyncChannelServer httpServer;
    public AsyncChannelServer iiopServer;
    public SocketSyncChannelFactory channelFactory;
    public BlockingQueue resultSlot = new ArrayBlockingQueue(1);

    public void testIIOPAccept() throws Exception {
        serverPacketCounter.set(0);
        hitIIOPServer();
        String type = (String) resultSlot.poll(1000 * 5, TimeUnit.MILLISECONDS);
        assertEquals("IIOP", type);
        // Verify that a request when through the one port.
        assertTrue(serverPacketCounter.get()>0);
    }

    public void testHttpAccept() throws IOException, URISyntaxException, InterruptedException {
        serverPacketCounter.set(0);
        hitHttpServer();
        String type = (String) resultSlot.poll(1000 * 5 * 10000, TimeUnit.MILLISECONDS);
        assertEquals("HTTP", type);
        // Verify that a request when through the one port.
        assertTrue(serverPacketCounter.get()>0);
    }

    protected void hitHttpServer() throws IOException, MalformedURLException {
        URI connectURI = server.getConnectURI();
        String url = "http://" + connectURI.getHost() + ":" + connectURI.getPort() + "/index.action";
        log.info(url);
        InputStream is = new URL(url).openStream();
        StringBuffer b = new StringBuffer();
        int c;
        while ((c = is.read()) >= 0) {
            b.append((char) c);
        }

        log.info("HTTP response: " + b);
    }

    protected void hitIIOPServer() throws Exception {
        SyncChannel channel = channelFactory.openSyncChannel(server.getConnectURI());
        ((SocketMetadata)channel.getAdapter(SocketMetadata.class)).setTcpNoDelay(true);
        channel.write(new ByteArrayPacket("GIOPcrapcrap".getBytes("UTF-8")));
        channel.flush();
        channel.dispose();
    }

    public void testUnknownAccept() throws IOException, URISyntaxException, InterruptedException {
        SyncChannel channel = channelFactory.openSyncChannel(server.getConnectURI());
        ((SocketMetadata)channel.getAdapter(SocketMetadata.class)).setTcpNoDelay(true);
        channel
                .write(new ByteArrayPacket("Licensed under the Apache License, Version 2.0 (the \"License\")"
                        .getBytes("UTF-8")));
        channel.flush();
        String type = (String) resultSlot.poll(1000 * 5, TimeUnit.MILLISECONDS);
        assertNull(type);
        channel.dispose();
    }

    protected void setUp() throws Exception {
        channelFactory = new SocketSyncChannelFactory();
        ThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, new ThreadFactory(){
            int count=0;
            public Thread newThread(Runnable arg0) {
                return new Thread(arg0, "activeio:"+(count++));
            }});
        AsyncChannelFactory factory = SyncToAsyncChannelFactory.adapt(channelFactory,executor);

        AsyncChannelServer cs = factory.bindAsyncChannel(new URI("tcp://localhost:0"));
        cs = new FilterAsyncChannelServer(cs) {
            public void onAccept(Channel channel) {
                SyncChannel syncChannel = AsyncToSyncChannel.adapt(channel);                
                super.onAccept(new FilterSyncChannel(syncChannel) {
                    public org.activeio.Packet read(long timeout) throws IOException {
                        Packet packet = super.read(timeout);
                        if( packet!=null && packet.hasRemaining() )
                            serverPacketCounter.incrementAndGet();
                        return packet;
                    }
                });
            }
        };
        
        server = new OnePortAsyncChannelServer(cs);
        server.start();

        startHTTPServer();
        startIIOPServer();

        log.info("Running on: "+server.getConnectURI());
    }

    /**
     * @throws IOException
     * @throws NamingException
     */
    protected void startIIOPServer() throws Exception {
        iiopServer = server.bindAsyncChannel(IIOPRecognizer.IIOP_RECOGNIZER);
        iiopServer.setAcceptListener(new AcceptListener() {
            public void onAccept(Channel channel) {
                try {
                    log.info("Got a IIOP connection.");
                    resultSlot.offer("IIOP", 1, TimeUnit.MILLISECONDS);
                    channel.dispose();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            public void onAcceptError(IOException error) {
            }
        });
        iiopServer.start();
    }

    /**
     * @throws IOException
     * @throws Exception
     */
    protected void startHTTPServer() throws Exception {
        httpServer = server.bindAsyncChannel(HttpRecognizer.HTTP_RECOGNIZER);
        httpServer.setAcceptListener(new AcceptListener() {
            public void onAccept(Channel channel) {
                try {
                    log.info("Got a HTTP connection.");
                    resultSlot.offer("HTTP", 1, TimeUnit.MILLISECONDS);

                    byte data[] = ("HTTP/1.1 200 OK\r\n" + "Content-Type: text/html; charset=UTF-8\r\n" + "\r\n"
                            + "Hello World").getBytes("UTF-8");

                    ((SocketMetadata)channel.getAdapter(SocketMetadata.class)).setTcpNoDelay(true);
                    ((AsyncChannel) channel).write(new ByteArrayPacket(data));

                    channel.dispose();
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }

            public void onAcceptError(IOException error) {
            }
        });
        httpServer.start();
    }

    protected void tearDown() throws Exception {
        stopIIOPServer();
        stopHTTPServer();
        server.dispose();
    }

    /**
     * @throws InterruptedException
     * 
     */
    protected void stopHTTPServer() throws InterruptedException {
        httpServer.dispose();
    }

    /**
     * @throws Exception
     * 
     */
    protected void stopIIOPServer() throws Exception {
        iiopServer.dispose();
    }
}
