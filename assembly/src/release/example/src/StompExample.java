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
import org.apache.activemq.transport.stomp.Stomp;
import org.apache.activemq.transport.stomp.StompConnection;
import org.apache.activemq.transport.stomp.StompFrame;
import org.apache.activemq.transport.stomp.Stomp.Headers.Subscribe;

/**
 * 
 * This example demonstrates Stomp Java API
 * 
 * @version $Revision$
 *
 */
public class StompExample {

	public static void main(String args[]) throws Exception {
		StompConnection connection = new StompConnection();
		connection.open("localhost", 61613);
		
		connection.connect("system", "manager");
		StompFrame connect = connection.receive();
		if (!connect.getAction().equals(Stomp.Responses.CONNECTED)) {
			throw new Exception ("Not connected");
		}
		
		connection.begin("tx1");
		connection.send("/queue/test", "message1");
		connection.send("/queue/test", "message2");
		connection.commit("tx1");
		
		connection.subscribe("/queue/test", Subscribe.AckModeValues.CLIENT);
		
		connection.begin("tx2");
		
		StompFrame message = connection.receive();
		System.out.println(message.getBody());
		connection.ack(message, "tx2");
		
		message = connection.receive();
		System.out.println(message.getBody());
		connection.ack(message, "tx2");
		
		connection.commit("tx2");
		
		connection.disconnect();
	}
	
}
