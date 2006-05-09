/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
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
package org.apache.activemq.example;

import javax.ejb.MessageDrivenBean;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.naming.InitialContext;

import weblogic.ejb.GenericMessageDrivenBean;
import weblogic.ejbgen.ActivationConfigProperties;
import weblogic.ejbgen.ActivationConfigProperty;
import weblogic.ejbgen.MessageDriven;
import weblogic.ejbgen.ResourceRef;

/**
 * A simple MDB that demonstrates receiving and sending messages via ActiveMQ.
 * 
 */
@MessageDriven(ejbName = "CounterMDB", destinationType = "javax.jms.Queue", resourceAdapterJndiName = "activemq/ResourceAdapter", maxSuspendSeconds = 0)
@ActivationConfigProperties(value = { @ActivationConfigProperty(name = "destination", value = "BAR") })
@ResourceRef(jndiName = "activemq/ConnectionFactory", type = "javax.jms.ConnectionFactory", auth = ResourceRef.Auth.APPLICATION, name = "cf", id = "cf", sharingScope = ResourceRef.SharingScope.SHAREABLE)
public class CounterMDB extends GenericMessageDrivenBean implements
		MessageDrivenBean, MessageListener {

	private static final long serialVersionUID = 1L;

	private static int counter = 0;

	public void onMessage(Message msg) {
		System.out.println("Got: " + msg);
		incrementCounter();

		// Try to forward the Message.
		try {
			InitialContext ic = new InitialContext();
			ConnectionFactory cf = (ConnectionFactory) ic.lookup("java:comp/env/cf");
			ic.close();
			Connection connection = cf.createConnection();
			try {
				Session session = connection.createSession(false, 0);
				MessageProducer producer = session.createProducer(session.createQueue("FOO"));
				producer.send(msg);
			} finally {
				connection.close();
			}
			
		} catch (Exception e) {
			System.out.println("Could not forward the message.");
			e.printStackTrace();
		}
	}

	public static synchronized void incrementCounter() {
		counter++;
	}

	public static synchronized int getCounter() {
		return counter;
	}
}