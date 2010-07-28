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
import java.net.ServerSocket;
import java.util.Random;
import javax.net.ServerSocketFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 */
public class ServerSocketTstFactory extends ServerSocketFactory {
    private static final Log LOG = LogFactory.getLog(ServerSocketTstFactory.class);

    private class ServerSocketTst {

	private final	ServerSocket	socket;

	public ServerSocketTst(int port, Random rnd) throws IOException {
		this.socket = ServerSocketFactory.getDefault().createServerSocket(port);
	}

	public ServerSocketTst(int port, int backlog, Random rnd) throws IOException {
		this.socket = ServerSocketFactory.getDefault().createServerSocket(port, backlog);
	}

	public ServerSocketTst(int port, int backlog, InetAddress bindAddr, Random rnd) throws IOException {
		this.socket = ServerSocketFactory.getDefault().createServerSocket(port, backlog, bindAddr);
	}

	public ServerSocket	getSocket() {
		return this.socket;
	}
    };

   private final Random	rnd;

   public ServerSocketTstFactory() {
	super();
	LOG.info("Creating a new ServerSocketTstFactory");
	this.rnd = new Random();
   }

   public ServerSocket createServerSocket(int port) throws IOException {
	ServerSocketTst	sSock = new ServerSocketTst(port, this.rnd);
	return sSock.getSocket();
   }

   public ServerSocket createServerSocket(int port, int backlog) throws IOException {
	ServerSocketTst	sSock = new ServerSocketTst(port, backlog, this.rnd);
	return sSock.getSocket();
   }

   public ServerSocket createServerSocket(int port, int backlog, InetAddress ifAddress) throws IOException {
	ServerSocketTst	sSock = new ServerSocketTst(port, backlog, ifAddress, this.rnd);
	return sSock.getSocket();
   }

   private final static ServerSocketTstFactory server = new ServerSocketTstFactory();

   public static ServerSocketTstFactory getDefault() {
	return server;
   }
}
