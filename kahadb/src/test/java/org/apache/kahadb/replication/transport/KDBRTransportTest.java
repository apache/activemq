/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kahadb.replication.transport;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportAcceptListener;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportListener;
import org.apache.activemq.transport.TransportServer;
import org.apache.kahadb.replication.ReplicationFrame;
import org.apache.kahadb.replication.pb.PBHeader;
import org.apache.kahadb.replication.pb.PBJournalLocation;
import org.apache.kahadb.replication.pb.PBSlaveInit;
import org.apache.kahadb.replication.pb.PBType;

public class KDBRTransportTest extends TestCase {

	private static final String KDBR_URI = "kdbr://localhost:61618";
	private List<Object> serverQueue;
	private List<Object> clientQueue;
	private List<Transport> serverTransports;
	private TransportServer server;
	private Transport client;
	
	private Object commandLatchMutex = new Object();
	private CountDownLatch commandLatch;
	
	protected void releaseCommandLatch() {
		synchronized( commandLatchMutex ) {
			if( commandLatch == null ) {
				return;
			}
			commandLatch.countDown();
			commandLatch=null;
		}
	}
	
	protected CountDownLatch getCommandLatch() {
		synchronized( commandLatchMutex ) {
			if( commandLatch == null ) {
				commandLatch = new CountDownLatch(1);
			}
			return commandLatch;
		}
	}
	
	@Override
	protected void setUp() throws Exception {
		serverQueue = Collections.synchronizedList(new ArrayList<Object>()); 
		clientQueue = Collections.synchronizedList(new ArrayList<Object>()); 
		serverTransports = Collections.synchronizedList(new ArrayList<Transport>()); 
		
		// Setup a server
		server = TransportFactory.bind(new URI(KDBR_URI));
		server.setAcceptListener(new TransportAcceptListener() {
			public void onAccept(Transport transport) {
				try {
					transport.setTransportListener(new TransportListener() {
						public void onCommand(Object command) {
							try {
								serverQueue.add(command);
								process(command);
								releaseCommandLatch();
							} catch (IOException e) {
								onException(e);
							}
						}

						public void onException(IOException error) {
							serverQueue.add(error);
							serverTransports.remove(this);
							releaseCommandLatch();
						}

						public void transportInterupted() {
						}

						public void transportResumed() {
						}
					});
					transport.start();
					serverTransports.add(transport);
				} catch (Exception e) {
					onAcceptError(e);
				}
			}

			public void onAcceptError(Exception error) {
				error.printStackTrace();
			}
		});
		server.start();

		// Connect a client.
		client = TransportFactory.connect(new URI(KDBR_URI));
		client.setTransportListener(new TransportListener() {
			public void onCommand(Object command) {
				clientQueue.add(command);
				releaseCommandLatch();
			}

			public void onException(IOException error) {
				clientQueue.add(error);
				releaseCommandLatch();
			}

			public void transportInterupted() {
			}

			public void transportResumed() {
			}
		});
		client.start();	
	}
	
	@Override
	protected void tearDown() throws Exception {
		client.stop();
		server.stop();
	}

	private void process(Object command) throws IOException {		
		ReplicationFrame frame = (ReplicationFrame) command;
		// Since we are processing the commands async in this test case we need to full read the stream before
		// returning since will be be used to read the next command once we return.
		if( frame.getHeader().getType() == PBType.FILE_TRANSFER_RESPONSE ) {
			InputStream ais = (InputStream) frame.getPayload();
			byte actualPayload[] = new byte[(int)frame.getHeader().getPayloadSize()];
			readFully(ais, actualPayload);
			frame.setPayload(actualPayload);
		}
	}
	
	/**
	 * Test a frame that has a streaming payload.
	 * 
	 * @throws Exception
	 */
	public void testFileTransferResponse() throws Exception {

		byte expectedPayload[] = {1,2,3,4,5,6,7,8,9,10}; 

		ReplicationFrame expected = new ReplicationFrame();
		expected.setHeader(new PBHeader().setType(PBType.FILE_TRANSFER_RESPONSE).setPayloadSize(expectedPayload.length));
		ByteArrayInputStream is = new ByteArrayInputStream(expectedPayload);
		expected.setPayload(is);
		
		CountDownLatch latch = getCommandLatch();
		client.oneway(expected);
		is.close();
		latch.await(2, TimeUnit.SECONDS);
		
		assertEquals(1, serverQueue.size());
		ReplicationFrame actual = (ReplicationFrame) serverQueue.remove(0);
		
		assertEquals(expected.getHeader(), actual.getHeader());		
		assertTrue(Arrays.equals(expectedPayload, (byte[])actual.getPayload()));
		
	}

	
	/**
	 * Test out sending a frame that has a PB payload.
	 * 
	 * @throws Exception
	 */
	public void testPBSlaveInitFrame() throws Exception {


		ReplicationFrame expected = new ReplicationFrame();
		expected.setHeader(new PBHeader().setType(PBType.SLAVE_INIT));
		expected.setPayload(new PBSlaveInit().setNodeId("foo"));
		
		CountDownLatch latch = getCommandLatch();
		client.oneway(expected);
		latch.await(2, TimeUnit.SECONDS);
		
		assertEquals(1, serverQueue.size());
		ReplicationFrame actual = (ReplicationFrame) serverQueue.remove(0);
		
		assertEquals(expected.getHeader(), actual.getHeader());
		assertEquals(expected.getPayload(), actual.getPayload());
		
	}


	private void readFully(InputStream ais, byte[] actualPayload) throws IOException {
		int pos = 0;
		int c;
		while( pos < actualPayload.length && (c=ais.read(actualPayload, pos, actualPayload.length-pos))>=0 ) {
			pos += c;
		}
		if( pos  < actualPayload.length ) {
			throw new EOFException();
		}
	}
}
