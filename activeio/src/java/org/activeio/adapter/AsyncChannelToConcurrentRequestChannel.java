/**
 *
 * Copyright 2004 Hiram Chirino
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.activeio.adapter;

import java.io.IOException;
import java.io.InterruptedIOException;

import org.activeio.AsyncChannel;
import org.activeio.ChannelFactory;
import org.activeio.FilterAsyncChannel;
import org.activeio.Packet;
import org.activeio.PacketData;
import org.activeio.RequestChannel;
import org.activeio.RequestListener;
import org.activeio.packet.AppendedPacket;
import org.activeio.packet.ByteArrayPacket;

import edu.emory.mathcs.backport.java.util.concurrent.ArrayBlockingQueue;
import edu.emory.mathcs.backport.java.util.concurrent.ConcurrentHashMap;
import edu.emory.mathcs.backport.java.util.concurrent.Executor;
import edu.emory.mathcs.backport.java.util.concurrent.TimeUnit;


/**
 * Creates a {@see org.activeio.RequestChannel} out of a {@see org.activeio.AsyncChannel}.  This 
 * {@see org.activeio.RequestChannel} is thread safe and mutiplexes concurrent requests and responses over
 * the underlying {@see org.activeio.AsyncChannel}.
 * 
 * @version $Revision$
 */
final public class AsyncChannelToConcurrentRequestChannel extends FilterAsyncChannel implements RequestChannel {

    private static final byte PASSTHROUGH = 0x00;
    private static final byte REQUEST = 0x01;
    private static final byte RESPONSE = 0x02;    
    private static final ByteArrayPacket PASSTHROUGH_PACKET = new ByteArrayPacket(new byte[]{PASSTHROUGH});
    
    private final ConcurrentHashMap requestMap = new ConcurrentHashMap();
    private final Executor requestExecutor;
    private short nextRequestId = 0;
    private final Object writeMutex = new Object();
    
    private RequestListener requestListener;
    
    public AsyncChannelToConcurrentRequestChannel(AsyncChannel next) {
        this(next, ChannelFactory.DEFAULT_EXECUTOR);
    }

    public AsyncChannelToConcurrentRequestChannel(AsyncChannel next, Executor requestExecutor) {
        super(next);
        this.requestExecutor=requestExecutor;
    }
    
    synchronized short getNextRequestId() {
        return nextRequestId++;
    }

    /**
     * @see org.activeio.FilterAsyncChannel#write(org.activeio.Packet)
     */
    public void write(Packet packet) throws IOException {
        Packet passThrough = AppendedPacket.join(PASSTHROUGH_PACKET.duplicate(), packet);
        synchronized(writeMutex) {
            super.write(passThrough);
        }
    }

    /**
     * @see org.activeio.FilterAsyncChannel#onPacket(org.activeio.Packet)
     */
    public void onPacket(final Packet packet) {
            switch( packet.read() ) {
            	case PASSTHROUGH:
                    super.onPacket(packet);
                    break;
            	case REQUEST:
                    requestExecutor.execute(new Runnable(){
                        public void run() {
                    	    serviceRequest(packet);
                        }
                    });
            	    break;
            	case RESPONSE:
                    serviceReponse(packet);
            	    break;
            }
    }

    private void serviceRequest(Packet packet) {
        try {
            if( requestListener ==null )
                throw new IOException("The RequestListener has not been set.");

            PacketData data = new PacketData(packet);
            short requestId = data.readShort();            
            Packet reponse = requestListener.onRequest(packet);

            // Send the response...
            Packet header = createHeaderPacket(RESPONSE, requestId);        
            Packet rc = AppendedPacket.join(header, packet);        
            synchronized(writeMutex) {
                super.write(rc);
            }
        } catch (IOException e) {
            super.onPacketError(e);
        }
        
    }

    private void serviceReponse(Packet packet) {
        
        try {
            
            PacketData data = new PacketData(packet);
            short requestId = data.readShort();
            
            ArrayBlockingQueue responseSlot = (ArrayBlockingQueue) requestMap.get(new Short(requestId));
            responseSlot.put(packet);
            
        } catch (IOException e) {
            super.onPacketError(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        
    }

    public Packet request(Packet request, long timeout) throws IOException {
        
        Short requestId = new Short(getNextRequestId());
        ArrayBlockingQueue responseSlot = new ArrayBlockingQueue(1);
        requestMap.put(requestId, responseSlot);
        
        Packet header = createHeaderPacket(REQUEST, requestId.shortValue());        
        Packet packet = AppendedPacket.join(header, request);
        
        synchronized(writeMutex) {
            super.write(packet);
        }
        
        try {
            
            if( timeout == WAIT_FOREVER_TIMEOUT ) {
                return (Packet) responseSlot.take();                
            } else if (timeout == NO_WAIT_TIMEOUT ) {
                return (Packet) responseSlot.poll(1, TimeUnit.MILLISECONDS);                                
            } else {
                return (Packet) responseSlot.poll(timeout, TimeUnit.MILLISECONDS);                                
            }
            
        } catch (InterruptedException e) {
            throw new InterruptedIOException(e.getMessage());
        } finally {
            requestMap.remove(requestId);
        }        
    }

    private Packet createHeaderPacket(byte type, short requestId) throws IOException {
        ByteArrayPacket header = new ByteArrayPacket(new byte[]{3});
        PacketData data = new PacketData(header);
        data.writeByte(type);
        data.writeShort(requestId);
        header.flip();
        return header;
    }

    public void setRequestListener(RequestListener requestListener) throws IOException {
        this.requestListener = requestListener;        
    }

    public RequestListener getRequestListener() {
        return requestListener;
    }
}
