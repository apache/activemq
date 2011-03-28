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
package org.apache.activemq.transport.tcp;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import javax.net.SocketFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 *
 * Automatically generated socket.close() calls to simulate network faults
 */
public class SocketTstFactory extends SocketFactory {
    private static final Logger LOG = LoggerFactory.getLogger(SocketTstFactory.class);

    private static final ConcurrentHashMap<InetAddress, Integer>	closeIter = new ConcurrentHashMap<InetAddress, Integer>();

    private class SocketTst {

	private class Bagot implements Runnable {
		private Thread			processus;
		private Random	rnd;
		private Socket			socket;
		private final InetAddress	address;

		public Bagot(Random rnd, Socket socket, InetAddress address) {
			this.processus  = new Thread(this, "Network Faults maker : undefined");
			this.rnd	= rnd;
			this.socket	= socket;
			this.address	= address;
		}

		public void start() {
			this.processus.setName("Network Faults maker : " + this.socket.toString());
			this.processus.start();
		}

		public void run () {
			int 	lastDelayVal;
			Integer lastDelay;
			while (!this.processus.isInterrupted()) {
				if (!this.socket.isClosed()) {
					try {
						lastDelay = closeIter.get(this.address);
						if (lastDelay == null) { 
							lastDelayVal = 0;
						}
						else {
							lastDelayVal = lastDelay.intValue();
							if (lastDelayVal > 10)
								lastDelayVal += 20;
							else	lastDelayVal += 1;	
						}

						lastDelay = new Integer(lastDelayVal);

						LOG.info("Trying to close client socket " + socket.toString() +  " in " + lastDelayVal + " milliseconds");

						try {
							Thread.sleep(lastDelayVal);
						} catch (InterruptedException e) {
							this.processus.interrupt();
							Thread.currentThread().interrupt();
						} catch (IllegalArgumentException e) {
						}
							
						this.socket.close();
						closeIter.put(this.address, lastDelay);
						LOG.info("Client socket " + this.socket.toString() + " is closed.");
					} catch (IOException e) {
					}
				}

				this.processus.interrupt();
			}
		}
	}

	private	final Bagot		bagot;
	private final Socket		socket;

	public SocketTst(InetAddress address, int port, Random rnd) throws IOException {
		this.socket = new Socket(address, port);
		bagot = new Bagot(rnd, this.socket, address);
	}

	public SocketTst(InetAddress address, int port, InetAddress localAddr, int localPort, Random rnd) throws IOException {
		this.socket = new Socket(address, port, localAddr, localPort);
		bagot = new Bagot(rnd, this.socket, address);
	}

	public SocketTst(String address, int port, Random rnd) throws UnknownHostException, IOException {
		this.socket = new Socket(address, port);
		bagot = new Bagot(rnd, this.socket, InetAddress.getByName(address));
	}

	public SocketTst(String address, int port, InetAddress localAddr, int localPort, Random rnd) throws IOException {
		this.socket = new Socket(address, port, localAddr, localPort);
		bagot = new Bagot(rnd, this.socket, InetAddress.getByName(address));
	}

	public Socket getSocket() {
		return this.socket;
	}

	public void startBagot() {
		bagot.start();
	}
    };

    private final Random		rnd;

    public SocketTstFactory() {
	super();
	LOG.info("Creating a new SocketTstFactory");
	this.rnd	= new Random();
    }

    public Socket createSocket(InetAddress host, int port) throws IOException {
	SocketTst sockTst;
	sockTst = new SocketTst(host, port, this.rnd);
	sockTst.startBagot();
	return sockTst.getSocket();
    }

    public Socket createSocket(InetAddress host, int port, InetAddress localAddress, int localPort) throws IOException {
	SocketTst	sockTst;
	sockTst = new SocketTst(host, port, localAddress, localPort, this.rnd);
	sockTst.startBagot();
	return sockTst.getSocket();
    }

    public Socket createSocket(String host, int port) throws IOException {
	SocketTst	sockTst;
	sockTst = new SocketTst(host, port, this.rnd);
	sockTst.startBagot();
	return sockTst.getSocket();
    }

    public Socket createSocket(String host, int port, InetAddress localAddress, int localPort) throws IOException {
	SocketTst	sockTst;
	sockTst = new SocketTst(host, port, localAddress, localPort, this.rnd);
	sockTst.startBagot();
	return sockTst.getSocket();
    }

    private final static SocketTstFactory client = new SocketTstFactory();

    public static SocketFactory getDefault() {
	return client;
    }
}
