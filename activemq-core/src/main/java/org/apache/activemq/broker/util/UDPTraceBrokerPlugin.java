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
package org.apache.activemq.broker.util;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;

import org.apache.activeio.command.WireFormat;
import org.apache.activeio.command.WireFormatFactory;
import org.apache.activeio.packet.ByteSequence;
import org.apache.activeio.util.ByteArrayOutputStream;
import org.apache.activemq.broker.BrokerPluginSupport;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.BrokerId;
import org.apache.activemq.command.DataStructure;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.JournalTrace;
import org.apache.activemq.openwire.OpenWireFormatFactory;

/**
 * A Broker interceptor which allows you to trace all operations to a UDP socket.
 * 
 * @org.apache.xbean.XBean
 * 
 * @version $Revision: 427613 $
 */
public class UDPTraceBrokerPlugin extends BrokerPluginSupport {

	protected WireFormat wireFormat;
	protected WireFormatFactory wireFormatFactory;
	protected int maxTraceDatagramSize = 1024*4;
	protected URI destination;
	protected DatagramSocket socket;
		
	protected BrokerId brokerId;
	protected SocketAddress address;
	protected boolean broadcast;
	
	public UDPTraceBrokerPlugin() {
		try {
			destination = new URI("udp://127.0.0.1:61616");
		} catch (URISyntaxException wontHappen) {
		}
	}

	public void start() throws Exception {
		super.start();
		if( getWireFormat() == null )
			throw new IllegalArgumentException("Wireformat must be specifed.");	
		if( address == null ) {
			address = createSocketAddress(destination);
		}
		socket = createSocket();
		
		brokerId = super.getBrokerId();
		trace(new JournalTrace("START"));		
	}	

	protected DatagramSocket createSocket() throws IOException {
		DatagramSocket s = new DatagramSocket();
		s.setSendBufferSize(maxTraceDatagramSize);
		s.setBroadcast(broadcast);
		return s;
	}

	public void stop() throws Exception {
		trace(new JournalTrace("STOP"));
		socket.close();
		super.stop();
	}
	
	private void trace(DataStructure command) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream(maxTraceDatagramSize);
		DataOutputStream out = new DataOutputStream(baos);
		wireFormat.marshal(brokerId, out);
		wireFormat.marshal(command, out);
		out.close();
		ByteSequence sequence = baos.toByteSequence();
		DatagramPacket datagram = new DatagramPacket( sequence.getData(), sequence.getOffset(), sequence.getLength(), address);		
		socket.send(datagram);
	}
	
    public void send(ConnectionContext context, Message messageSend) throws Exception {
    	trace(messageSend);
        super.send(context, messageSend);
    }

    public void acknowledge(ConnectionContext context, MessageAck ack) throws Exception {
    	trace(ack);
        super.acknowledge(context, ack);
    }

	public WireFormat getWireFormat() {
		if( wireFormat == null ) {
			wireFormat = createWireFormat();
		}
		return wireFormat;
	}

	protected WireFormat createWireFormat() {
		return getWireFormatFactory().createWireFormat();
	}

	public void setWireFormat(WireFormat wireFormat) {
		this.wireFormat = wireFormat;
	}

	public WireFormatFactory getWireFormatFactory() {
		if( wireFormatFactory == null ) {
			wireFormatFactory = createWireFormatFactory();
		}
		return wireFormatFactory;
	}

	protected OpenWireFormatFactory createWireFormatFactory() {
		OpenWireFormatFactory wf = new OpenWireFormatFactory();
		wf.setCacheEnabled(false);
		wf.setVersion(1);
		wf.setTightEncodingEnabled(true);
		wf.setSizePrefixDisabled(true);
		return wf;
	}

	public void setWireFormatFactory(WireFormatFactory wireFormatFactory) {
		this.wireFormatFactory = wireFormatFactory;
	}


	protected SocketAddress createSocketAddress(URI location) throws UnknownHostException {
		InetAddress a = InetAddress.getByName(location.getHost());
		int port = location.getPort();
		return new InetSocketAddress(a, port);
	}

	public URI getDestination() {
		return destination;
	}

	public void setDestination(URI destination) {
		this.destination = destination;
	}

	public int getMaxTraceDatagramSize() {
		return maxTraceDatagramSize;
	}

	public void setMaxTraceDatagramSize(int maxTraceDatagramSize) {
		this.maxTraceDatagramSize = maxTraceDatagramSize;
	}

	public boolean isBroadcast() {
		return broadcast;
	}

	public void setBroadcast(boolean broadcast) {
		this.broadcast = broadcast;
	}

	public SocketAddress getAddress() {
		return address;
	}

	public void setAddress(SocketAddress address) {
		this.address = address;
	}


}
