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

package org.apache.activemq.transport.stomp;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;


public class StompConnection {

    public static final long RECEIVE_TIMEOUT = 10000;
    
    private Socket stompSocket;
    private ByteArrayOutputStream inputBuffer = new ByteArrayOutputStream();

    public void open(String host, int port) throws IOException, UnknownHostException {
        stompSocket = new Socket(host, port);
    }

	public void close() throws IOException {
		if (stompSocket != null) {
		    stompSocket.close();
	        stompSocket = null;
		}
	}
	
    public void sendFrame(String data) throws Exception {
        byte[] bytes = data.getBytes("UTF-8");
        OutputStream outputStream = stompSocket.getOutputStream();
        outputStream.write(bytes);
        outputStream.write(0);
        outputStream.flush();
    }

    public String receiveFrame() throws Exception {
    	return receiveFrame(RECEIVE_TIMEOUT);
    }

    private String receiveFrame(long timeOut) throws Exception {
        stompSocket.setSoTimeout((int) timeOut);
        InputStream is = stompSocket.getInputStream();
        int c = 0;
        for (;;) {
            c = is.read();
            if (c < 0) {
                throw new IOException("socket closed.");
            }
            else if (c == 0) {
                c = is.read();
                if (c != '\n') {
                	throw new IOException("Expecting stomp frame to terminate with \0\n");
                }
                byte[] ba = inputBuffer.toByteArray();
                inputBuffer.reset();
                return new String(ba, "UTF-8");
            }
            else {
                inputBuffer.write(c);
            }
        }
    }

}
