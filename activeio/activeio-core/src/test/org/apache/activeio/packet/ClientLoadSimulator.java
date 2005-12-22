/**
 *
 * Copyright 2004 The Apache Software Foundation
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
package org.apache.activeio.packet;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.activeio.ChannelFactory;
import org.apache.activeio.packet.ByteArrayPacket;
import org.apache.activeio.packet.EOSPacket;
import org.apache.activeio.packet.Packet;
import org.apache.activeio.packet.sync.SyncChannel;
import org.apache.activeio.stats.CountStatisticImpl;
import org.apache.activeio.stats.TimeStatisticImpl;
import org.apache.commons.beanutils.BeanUtils;

import edu.emory.mathcs.backport.java.util.concurrent.CountDownLatch;
import edu.emory.mathcs.backport.java.util.concurrent.LinkedBlockingQueue;
import edu.emory.mathcs.backport.java.util.concurrent.SynchronousQueue;
import edu.emory.mathcs.backport.java.util.concurrent.ThreadPoolExecutor;
import edu.emory.mathcs.backport.java.util.concurrent.TimeUnit;

/**
 * Simulates multiple a simple tcp echo clients for use in benchmarking activeio
 * channel implementations.
 * 
 * @version $Revision$
 */
public class ClientLoadSimulator implements Runnable {

    private URI url;

    // Afects how clients are created
    private int concurrentClients = 10;
    private long rampUpTime = 1000 * concurrentClients;

    // Afects how clients behave
    private long requestDelay = 500;
    private int requestIterations = Integer.MAX_VALUE;
    private int requestSize = 1024;
    
    ThreadPoolExecutor threadPool = new ThreadPoolExecutor(10, Integer.MAX_VALUE, 5, TimeUnit.SECONDS, new SynchronousQueue());

    // The packet the clients send to the server.
    private Packet requestPacket;
    private long sampleInterval = 1000;
    private ChannelFactory factory = new ChannelFactory();
    private CountDownLatch shutdownCountDownLatch;

    private final CountStatisticImpl activeConnectionsCounter = new CountStatisticImpl("activeConnectionsCounter",
            "The number of active connection attached to the server.");
    private final CountStatisticImpl echoedBytesCounter = new CountStatisticImpl("echoedBytesCounter",
            "The number of bytes that have been echoed by the server.");
    private final TimeStatisticImpl requestLatency = new TimeStatisticImpl("requestLatency",
            "The amount of time that is spent waiting for a request to be serviced");

    public static void main(String[] args) throws URISyntaxException, IllegalAccessException, InvocationTargetException {

        ClientLoadSimulator client = new ClientLoadSimulator();

        HashMap options = new HashMap();
        for (int i = 0; i < args.length; i++) {

            String option = args[i];
            if (!option.startsWith("-") || option.length() < 2 || i + 1 >= args.length) {
                System.out.println("Invalid usage");
                return;
            }

            option = option.substring(1);
            options.put(option, args[++i]);
        }

        BeanUtils.populate(client, options);
        
        System.out.println();
        System.out.println("Server starting with the following options: ");
        System.out.println(" url="+client.getUrl());
        System.out.println(" sampleInterval="+client.getSampleInterval());
        System.out.println(" concurrentClients="+client.getConcurrentClients());
        System.out.println(" rampUpTime="+client.getRampUpTime());
        System.out.println(" requestIterations="+client.getRequestIterations());
        System.out.println(" requestSize="+client.getRequestSize());
        System.out.println(" requestDelay="+client.getRequestDelay());
        System.out.println();
        client.run();

    }
    private void printSampleData() {
        long now = System.currentTimeMillis();
        float runDuration = (now - activeConnectionsCounter.getStartTime()) / 1000f;
        System.out.println("Active connections: " + activeConnectionsCounter.getCount());
        System.out.println("Echoed bytes: " + (echoedBytesCounter.getCount()/1024f) + " kb"
                + ", Request latency: " + requestLatency.getAverageTime()+" ms");
        echoedBytesCounter.reset();
        requestLatency.reset();
    }

    public void run() {
        ArrayList clients = new ArrayList();
        try {

            shutdownCountDownLatch = new CountDownLatch(1);
            activeConnectionsCounter.reset();
            echoedBytesCounter.reset();

            new Thread("Sampler") {
                public void run() {
                    System.out.println("Sampler started.");
                    try {
                        while (!shutdownCountDownLatch.await(sampleInterval, TimeUnit.MILLISECONDS)) {
                            printSampleData();
                        }
                    } catch (InterruptedException e) {
                    }
                    System.out.println("Sampler stopped.");
                }
            }.start();

            byte data[] = new byte[requestSize];
            for (int i = 0; i < data.length; i++) {
                data[i] = (byte) i;
            }
            requestPacket = new ByteArrayPacket(data);

            // Loop to ramp up the clients.

            long clientActivationDelay = rampUpTime / concurrentClients;
            for (int i = 0; i < concurrentClients && !shutdownCountDownLatch.await(clientActivationDelay, TimeUnit.MILLISECONDS); i++) {
                System.out.println("Adding Client: " + i);
                Client client = new Client();
                clients.add(client);
                new Thread(client, "Client: " + i).start();
            }

            shutdownCountDownLatch.await();

        } catch (InterruptedException e) {
        } finally {
            System.out.println("Shutting down clients.");
            for (Iterator iter = clients.iterator(); iter.hasNext();) {
                Client client = (Client) iter.next();
                client.dispose();
            }
        }
    }

    public String getUrl() {
        return url.toString();
    }

    public void setUrl(String url) throws URISyntaxException {
        this.url = new URI(url);
    }

    class Client implements Runnable {

        private CountDownLatch shutdownCountDownLatch = new CountDownLatch(1);

        Packet packet = requestPacket.duplicate();

        private SyncChannel syncChannel;

        public void run() {
            try {
                System.out.println("Client started.");

                activeConnectionsCounter.increment();
                syncChannel = factory.openSyncChannel(url);
                for (int i = 0; i < requestIterations && !shutdownCountDownLatch.await(1, TimeUnit.MILLISECONDS) ; i++) {

                    long start = System.currentTimeMillis();
                    sendRequest();
                    long end = System.currentTimeMillis();

                    requestLatency.addTime(end - start);
                    echoedBytesCounter.add(packet.remaining());

                    if( requestDelay > 0 ) {
                        Thread.sleep(requestDelay);
                    }
                }

            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                System.out.println("Client stopped.");

                activeConnectionsCounter.decrement();
                if( syncChannel!=null ) {
                    syncChannel.dispose();
                    syncChannel = null;
                }
            }
        }

        private void sendRequest() throws IOException, InterruptedException {
            
            final CountDownLatch done = new CountDownLatch(1);
            
            // Read the data async to avoid dead locks due buffers being to small for 
            // data being sent.
            threadPool.execute(new Runnable() {
                public void run() {
                    try {
                        int c = 0;
                        while (c < packet.remaining()) {
                            Packet p = syncChannel.read(1000*5);
                            if( p==null ) {
                                continue;
                            }
                            if( p == EOSPacket.EOS_PACKET ) {
                                System.out.println("Peer disconnected.");
                                dispose();
                            }
                            c += p.remaining();
                        }
                        done.countDown();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });

            syncChannel.write(packet.duplicate());
            syncChannel.flush();
            done.await();
            
        }

        public void dispose() {
            shutdownCountDownLatch.countDown();
        }
    }

    /**
     * @return Returns the concurrentClients.
     */
    public int getConcurrentClients() {
        return concurrentClients;
    }

    /**
     * @param concurrentClients
     *            The concurrentClients to set.
     */
    public void setConcurrentClients(int concurrentClients) {
        this.concurrentClients = concurrentClients;
    }

    /**
     * @return Returns the rampUpTime.
     */
    public long getRampUpTime() {
        return rampUpTime;
    }

    /**
     * @param rampUpTime
     *            The rampUpTime to set.
     */
    public void setRampUpTime(long rampUpTime) {
        this.rampUpTime = rampUpTime;
    }

    /**
     * @return Returns the requestDelay.
     */
    public long getRequestDelay() {
        return requestDelay;
    }

    /**
     * @param requestDelay
     *            The requestDelay to set.
     */
    public void setRequestDelay(long requestDelay) {
        this.requestDelay = requestDelay;
    }

    /**
     * @return Returns the requestIterations.
     */
    public int getRequestIterations() {
        return requestIterations;
    }

    /**
     * @param requestIterations
     *            The requestIterations to set.
     */
    public void setRequestIterations(int requestIterations) {
        this.requestIterations = requestIterations;
    }

    /**
     * @return Returns the requestSize.
     */
    public int getRequestSize() {
        return requestSize;
    }

    /**
     * @param requestSize
     *            The requestSize to set.
     */
    public void setRequestSize(int requestSize) {
        this.requestSize = requestSize;
    }    

    /**
     * @return Returns the sampleInterval.
     */
    public long getSampleInterval() {
        return sampleInterval;
    }
    /**
     * @param sampleInterval The sampleInterval to set.
     */
    public void setSampleInterval(long sampleInterval) {
        this.sampleInterval = sampleInterval;
    }
}
