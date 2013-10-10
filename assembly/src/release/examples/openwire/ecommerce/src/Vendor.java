/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TemporaryQueue;

import org.apache.activemq.ActiveMQConnectionFactory;

/**
 * The Vendor synchronously, and in a single transaction, receives the
 * order from VendorOrderQueue and sends messages to the two Suppliers via
 * MonitorOrderQueue and StorageOrderQueue.
 * The responses are received asynchronously; when both responses come
 * back, the order confirmation message is sent back to the Retailer.
 */
public class Vendor implements Runnable, MessageListener {
	private String url;
	private String user;
	private String password;
	private	Session asyncSession;
	private int numSuppliers = 2;
	private Object supplierLock = new Object();
	
	public Vendor(String url, String user, String password) {
		this.url = url;
		this.user = user;
		this.password = password;
	}
	
	public void run() {
		ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(user, password, url);
		Session session = null;
		Destination orderQueue;
		Destination monitorOrderQueue;
		Destination storageOrderQueue;
		TemporaryQueue vendorConfirmQueue;
		MessageConsumer orderConsumer = null;
		MessageProducer monitorProducer = null;
		MessageProducer storageProducer = null;

		try {
			Connection connection = connectionFactory.createConnection();

			session = connection.createSession(true, Session.SESSION_TRANSACTED);
			orderQueue = session.createQueue("VendorOrderQueue");
			monitorOrderQueue = session.createQueue("MonitorOrderQueue");
			storageOrderQueue = session.createQueue("StorageOrderQueue");
			
			orderConsumer = session.createConsumer(orderQueue);
			monitorProducer = session.createProducer(monitorOrderQueue);
			storageProducer = session.createProducer(storageOrderQueue);
			
			Connection asyncconnection = connectionFactory.createConnection();
			asyncSession = asyncconnection.createSession(true, Session.SESSION_TRANSACTED);
			
			vendorConfirmQueue = asyncSession.createTemporaryQueue();
			MessageConsumer confirmConsumer = asyncSession.createConsumer(vendorConfirmQueue);
			confirmConsumer.setMessageListener(this);
			
			asyncconnection.start();

			connection.start();

		
			while (true) {
				Order order = null;
				try {
					Message inMessage = orderConsumer.receive();
					MapMessage message;
					if (inMessage instanceof MapMessage) {
						message = (MapMessage) inMessage;
						
					} else {
						// end of stream
						Message outMessage = session.createMessage();
						outMessage.setJMSReplyTo(vendorConfirmQueue);
						monitorProducer.send(outMessage);
						storageProducer.send(outMessage);
						session.commit();
						break;
					}
					
					// Randomly throw an exception in here to simulate a Database error
					// and trigger a rollback of the transaction
					if (new Random().nextInt(3) == 0) {
						throw new JMSException("Simulated Database Error.");
					}
					
					order = new Order(message);
					
					MapMessage orderMessage = session.createMapMessage();
					orderMessage.setJMSReplyTo(vendorConfirmQueue);
					orderMessage.setInt("VendorOrderNumber", order.getOrderNumber());
					int quantity = message.getInt("Quantity");
					System.out.println("Vendor: Retailer ordered " + quantity + " " + message.getString("Item"));
					
					orderMessage.setInt("Quantity", quantity);
					orderMessage.setString("Item", "Monitor");
					monitorProducer.send(orderMessage);
					System.out.println("Vendor: ordered " + quantity + " Monitor(s)");
					
					orderMessage.setString("Item", "HardDrive");
					storageProducer.send(orderMessage);
					System.out.println("Vendor: ordered " + quantity + " Hard Drive(s)");
					
					session.commit();
					System.out.println("Vendor: Comitted Transaction 1");
					
				} catch (JMSException e) {
					System.out.println("Vendor: JMSException Occured: " + e.getMessage());
					e.printStackTrace();
					session.rollback();
					System.out.println("Vendor: Rolled Back Transaction.");
				}
			}
			
			synchronized (supplierLock) {
				while (numSuppliers > 0) {
					try {
						supplierLock.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
			
			connection.close();
			asyncconnection.close();
		
		} catch (JMSException e) {
			e.printStackTrace();
		}

	}

	public void onMessage(Message message) {
		if (!(message instanceof MapMessage)) {
			synchronized(supplierLock) {
				numSuppliers--;
				supplierLock.notifyAll();
			}
			try {
				asyncSession.commit();
				return;
			} catch (JMSException e) {
				e.printStackTrace();
			}
		}
		
		int orderNumber = -1;
		try {
			MapMessage componentMessage = (MapMessage) message;
			
			orderNumber = componentMessage.getInt("VendorOrderNumber");
			Order order = Order.getOrder(orderNumber);
			order.processSubOrder(componentMessage);
			asyncSession.commit();
			
			if (! "Pending".equals(order.getStatus())) {
				System.out.println("Vendor: Completed processing for order " + orderNumber);
				
				MessageProducer replyProducer = asyncSession.createProducer(order.getMessage().getJMSReplyTo());
				MapMessage replyMessage = asyncSession.createMapMessage();
				if ("Fulfilled".equals(order.getStatus())) {
					replyMessage.setBoolean("OrderAccepted", true);
					System.out.println("Vendor: sent " + order.quantity + " computer(s)");
				} else {
					replyMessage.setBoolean("OrderAccepted", false);
					System.out.println("Vendor: unable to send " + order.quantity + " computer(s)");
				}
				replyProducer.send(replyMessage);
				asyncSession.commit();
				System.out.println("Vender: committed transaction 2");
			}
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}

	public static class Order {
		private static Map<Integer, Order> pendingOrders = new HashMap<Integer, Order>();
		private static int nextOrderNumber = 1;

		private int orderNumber;
		private int quantity;
		private MapMessage monitor = null;
		private MapMessage storage = null;
		private MapMessage message;
		private String status;
		
		public Order(MapMessage message) {
			this.orderNumber = nextOrderNumber++;
			this.message = message;
			try {
				this.quantity = message.getInt("Quantity");
			} catch (JMSException e) {
				e.printStackTrace();
				this.quantity = 0;
			}
			status = "Pending";
			pendingOrders.put(orderNumber, this);
		}
		
		public Object getStatus() {
			return status;
		}
		
		public int getOrderNumber() {
			return orderNumber;
		}
		
		public static int getOutstandingOrders() {
			return pendingOrders.size();
		}
		
		public static Order getOrder(int number) {
			return pendingOrders.get(number);
		}
		
		public MapMessage getMessage() {
			return message;
		}
		
		public void processSubOrder(MapMessage message) {
			String itemName = null;
			try {
				itemName = message.getString("Item");
			} catch (JMSException e) {
				e.printStackTrace();
			}
			
			if ("Monitor".equals(itemName)) {
				monitor = message;
			} else if ("HardDrive".equals(itemName)) {
				storage = message;
			}
			
			if (null != monitor && null != storage) {
				// Received both messages
				try {
					if (quantity > monitor.getInt("Quantity")) {
						status = "Cancelled";
					} else if (quantity > storage.getInt("Quantity")) {
						status = "Cancelled";
					} else {
						status = "Fulfilled";
					}
				} catch (JMSException e) {
					e.printStackTrace();
					status = "Cancelled";
				}
			}
		}		
	}

	public static void main(String[] args) {
		String url = "tcp://localhost:61616";
		String user = null;
		String password = null;
		
		if (args.length >= 1) {
			url = args[0];
		}
		
		if (args.length >= 2) {
			user = args[1];
		}

		if (args.length >= 3) {
			password = args[2];
		}
		
		Vendor v = new Vendor(url, user, password);
		
		new Thread(v, "Vendor").start();
	}	
}
