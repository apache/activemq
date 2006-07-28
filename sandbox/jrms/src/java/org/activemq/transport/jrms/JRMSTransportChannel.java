/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.activemq.transport.jrms;

import EDU.oswego.cs.dl.util.concurrent.SynchronizedBoolean;
import com.sun.multicast.reliable.RMException;
import com.sun.multicast.reliable.transport.RMPacketSocket;
import com.sun.multicast.reliable.transport.SessionDoneException;
import com.sun.multicast.reliable.transport.TransportProfile;
import com.sun.multicast.reliable.transport.lrmp.LRMPTransportProfile;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.activemq.io.WireFormat;
import org.activemq.message.Packet;
import org.activemq.transport.TransportChannelSupport;
import org.activemq.util.IdGenerator;

import javax.jms.JMSException;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.URI;

/**
 * A JRMS implementation of a TransportChannel
 *
 * @version $Revision$
 */
public class JRMSTransportChannel extends TransportChannelSupport implements Runnable {

    private static final int SOCKET_BUFFER_SIZE = 32 * 1024;
    private static final Log log = LogFactory.getLog(JRMSTransportChannel.class);

    private WireFormat wireFormat;
    private SynchronizedBoolean closed;
    private SynchronizedBoolean started;
    private Thread thread; //need to change this - and use a thread pool
    // need to see our own messages
    private RMPacketSocket socket;
    private IdGenerator idGenerator;
    private String channelId;
    private int port;
    private InetAddress inetAddress;
    private Object lock;

    /**
     * Construct basic helpers
     */
    protected JRMSTransportChannel(WireFormat wireFormat) {
        this.wireFormat = wireFormat;
        idGenerator = new IdGenerator();
        channelId = idGenerator.generateId();
        closed = new SynchronizedBoolean(false);
        started = new SynchronizedBoolean(false);
        lock = new Object();
    }

    /**
     * Connect to a remote Node - e.g. a Broker
     *
     * @param remoteLocation
     * @throws JMSException
     */
    public JRMSTransportChannel(WireFormat wireFormat, URI remoteLocation) throws JMSException {
        this(wireFormat);
        try {
            this.port = remoteLocation.getPort();
            this.inetAddress = InetAddress.getByName(remoteLocation.getHost());
            LRMPTransportProfile profile = new LRMPTransportProfile(inetAddress, port);
            profile.setTTL((byte) 1);
            profile.setOrdered(true);
            this.socket = profile.createRMPacketSocket(TransportProfile.SEND_RECEIVE);
        }
        catch (Exception ioe) {
            ioe.printStackTrace();
            JMSException jmsEx = new JMSException("Initialization of JRMSTransportChannel failed: " + ioe);
            jmsEx.setLinkedException(ioe);
            throw jmsEx;
        }
    }

    /**
     * close the channel
     */
    public void stop() {
        if (closed.commit(false, true)) {
            super.stop();
            try {
                socket.close();
            }
            catch (Exception e) {
                log.trace(toString() + " now closed");
            }
        }
    }

    /**
     * start listeneing for events
     *
     * @throws JMSException if an error occurs
     */
    public void start() throws JMSException {
        if (started.commit(false, true)) {
            thread = new Thread(this, toString());
            if (isServerSide()) {
                thread.setDaemon(true);
            }
            thread.start();
        }
    }

    /**
     * Asynchronously send a Packet
     *
     * @param packet
     * @throws JMSException
     */
    public void asyncSend(Packet packet) throws JMSException {
        try {
            DatagramPacket dpacket = createDatagramPacket(packet);

            // lets sync to avoid concurrent writes
            //synchronized (lock) {
            socket.send(dpacket);
            //}
        }
        catch (RMException rme) {
            JMSException jmsEx = new JMSException("syncSend failed " + rme.getMessage());
            jmsEx.setLinkedException(rme);
            throw jmsEx;
        }
        catch (IOException e) {
            JMSException jmsEx = new JMSException("asyncSend failed " + e.getMessage());
            jmsEx.setLinkedException(e);
            throw jmsEx;
        }
    }


    public boolean isMulticast() {
        return true;
    }

    /**
     * reads packets from a Socket
     */
    public void run() {
        try {
            while (!closed.get()) {
                DatagramPacket dpacket = socket.receive();
                Packet packet = wireFormat.readPacket(channelId, dpacket);
                if (packet != null) {
                    doConsumePacket(packet);
                }
            }
            log.trace("The socket peer is now closed");
            //doClose(new IOException("Socket peer is now closed"));
            stop();
        }
        catch (SessionDoneException e) {
            // this isn't really an exception, it just indicates
            // that the socket has closed normally
            log.trace("Session completed", e);
            stop();
        }
        catch (RMException ste) {
            doClose(ste);
        }
        catch (IOException e) {
            doClose(e);
        }
    }
    
    /**
     * Can this wireformat process packets of this version
     * @param version the version number to test
     * @return true if can accept the version
     */
    public boolean canProcessWireFormatVersion(int version){
        return wireFormat.canProcessWireFormatVersion(version);
    }
    
    /**
     * @return the current version of this wire format
     */
    public int getCurrentWireFormatVersion(){
        return wireFormat.getCurrentWireFormatVersion();
    }

    protected DatagramPacket createDatagramPacket() {
        DatagramPacket answer = new DatagramPacket(new byte[SOCKET_BUFFER_SIZE], SOCKET_BUFFER_SIZE);
        answer.setPort(port);
        answer.setAddress(inetAddress);
        return answer;
    }

    protected DatagramPacket createDatagramPacket(Packet packet) throws IOException, JMSException {
        DatagramPacket answer = wireFormat.writePacket(channelId, packet);
        answer.setPort(port);
        answer.setAddress(inetAddress);
        return answer;
    }

    private void doClose(Exception ex) {
        if (!closed.get()) {
            JMSException jmsEx = new JMSException("Error reading socket: " + ex);
            jmsEx.setLinkedException(ex);
            onAsyncException(jmsEx);
            stop();
        }
    }

    /**
     * pretty print for object
     *
     * @return String representation of this object
     */
    public String toString() {
        return "JRMSTransportChannel: " + socket;
    }

    public void forceDisconnect() {
	    // TODO: implement me.
		throw new RuntimeException("Not yet Implemented.");
	}
}