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
package org.activeio.packet;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

import org.activeio.AcceptListener;
import org.activeio.Channel;
import org.activeio.ChannelFactory;
import org.activeio.adapter.SyncToAsyncChannel;
import org.activeio.packet.EOSPacket;
import org.activeio.packet.Packet;
import org.activeio.packet.async.AsyncChannel;
import org.activeio.packet.async.AsyncChannelListener;
import org.activeio.packet.async.AsyncChannelServer;
import org.activeio.stats.CountStatisticImpl;
import org.apache.commons.beanutils.BeanUtils;

import edu.emory.mathcs.backport.java.util.concurrent.CountDownLatch;
import edu.emory.mathcs.backport.java.util.concurrent.TimeUnit;

/**
 * Implements a simple tcp echo server for use in benchmarking 
 * activeio channel implementations.
 * 
 * @version $Revision$
 */
public class Server implements Runnable, AcceptListener {

    private URI url;
    private CountDownLatch shutdownCountDownLatch;
    private long requestDelay = 0;
    private long sampleInterval = 1000;
    
    private final CountStatisticImpl activeConnectionsCounter = new CountStatisticImpl("activeConnectionsCounter","The number of active connection attached to the server.");
    private final CountStatisticImpl echoedBytesCounter = new CountStatisticImpl("echoedBytesCounter","The number of bytes that have been echoed by the server.");

    public static void main(String[] args) throws URISyntaxException, IllegalAccessException, InvocationTargetException {

        Server server = new Server();
        
        HashMap options = new HashMap();       
        for( int i=0; i < args.length; i++ ) {
            
            String option = args[i];
            if( !option.startsWith("-") || option.length()<2 || i+1 >= args.length ) {
                System.out.println("Invalid usage.");
                return;
            }
            
            option = option.substring(1);
            options.put(option, args[++i]);            
        }        
        BeanUtils.populate(server, options);
        
        System.out.println();
        System.out.println("Server starting with the following options: ");
        System.out.println(" url="+server.getUrl());
        System.out.println(" sampleInterval="+server.getSampleInterval());
        System.out.println(" requestDelay="+server.getRequestDelay());
        System.out.println();
        server.run();

    }

    private void printSampleData() {
        long now = System.currentTimeMillis();
        float runDuration = (now - activeConnectionsCounter.getStartTime())/1000f;
        System.out.println("Active connections: "+activeConnectionsCounter.getCount());
        System.out.println("Echoed bytes: " + (echoedBytesCounter.getCount()/1024f) + " kb");
        echoedBytesCounter.reset();
    }
    

    public void run() {
        try {
            
            activeConnectionsCounter.reset();
            echoedBytesCounter.reset();
            
            shutdownCountDownLatch = new CountDownLatch(1);
                
            ChannelFactory factory = new ChannelFactory();
            AsyncChannelServer server = factory.bindAsyncChannel(url);
            System.out.println("Server accepting connections on: "+server.getConnectURI());
            server.setAcceptListener(this);
            server.start();
            
            while(!shutdownCountDownLatch.await(sampleInterval, TimeUnit.MILLISECONDS)) {
                printSampleData();
            }
            
            System.out.println("Stopping server.");
            server.stop();
            server.dispose();
            
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
        }
    }

    public String getUrl() {
        return url.toString();
    }

    public void setUrl(String url) throws URISyntaxException {
        this.url = new URI(url);
    }

    class ServerConnectionHandler implements AsyncChannelListener {

        private final AsyncChannel asyncChannel;
        private boolean disposed;
        
        public ServerConnectionHandler(AsyncChannel asyncChannel) {
            this.asyncChannel = asyncChannel;
            activeConnectionsCounter.increment();
        }

        public void onPacket(Packet packet) {
            
            if( packet == EOSPacket.EOS_PACKET ) {
                System.out.println("Peer disconnected.");
                dispose();
                return;
            }
            
            try {
                if( requestDelay > 0 ) {
                    Thread.sleep(requestDelay);
                }
                
                echoedBytesCounter.add(packet.remaining());
                asyncChannel.write(packet);
                asyncChannel.flush();
                
            } catch (IOException e) {
                onPacketError(e);
            } catch (InterruptedException e) {
                System.out.println("Interrupted... Shutting down.");
                dispose();
            }
        }

        public void onPacketError(IOException error) {
            error.printStackTrace();
            dispose();
        }

        private void dispose() {
            if( !disposed ) {
                asyncChannel.dispose();
                activeConnectionsCounter.decrement();
                disposed=true;
            }
        }
    }
    
    public void onAccept(Channel channel) {
        try {
            
            AsyncChannel asyncChannel = SyncToAsyncChannel.adapt(channel);
            asyncChannel.setAsyncChannelListener(new ServerConnectionHandler(asyncChannel));
            asyncChannel.start();
            
        } catch (IOException e) {
            onAcceptError(e);
        }
    }

    public void onAcceptError(IOException error) {
        error.printStackTrace();
        shutdownCountDownLatch.countDown();
    }
    
    /**
     * @return Returns the requestDelay.
     */
    public long getRequestDelay() {
        return requestDelay;
    }
    /**
     * @param requestDelay The requestDelay to set.
     */
    public void setRequestDelay(long requestDelay) {
        this.requestDelay = requestDelay;
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
