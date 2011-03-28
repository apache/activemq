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
package org.apache.activemq.store.amq;

import java.io.File;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.activemq.command.ActiveMQBlobMessage;
import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.activemq.command.ActiveMQMapMessage;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQObjectMessage;
import org.apache.activemq.command.ActiveMQStreamMessage;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.DataStructure;
import org.apache.activemq.command.JournalQueueAck;
import org.apache.activemq.command.JournalTopicAck;
import org.apache.activemq.command.JournalTrace;
import org.apache.activemq.command.JournalTransaction;
import org.apache.activemq.kaha.impl.async.Location;
import org.apache.activemq.kaha.impl.async.ReadOnlyAsyncDataManager;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.apache.velocity.app.VelocityEngine;
import org.josql.Query;

/**
 * Allows you to view the contents of a Journal.
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class AMQJournalTool {

	private final ArrayList<File> dirs = new ArrayList<File>();
	private final WireFormat wireFormat = new OpenWireFormat();
	private final HashMap<String, String> resources = new HashMap<String, String>();

	private String messageFormat = "${location.dataFileId},${location.offset}|${type}|${record.destination}|${record.messageId}|${record.properties}|${body}";
	private String topicAckFormat = "${location.dataFileId},${location.offset}|${type}|${record.destination}|${record.clientId}|${record.subscritionName}|${record.messageId}";
	private String queueAckFormat = "${location.dataFileId},${location.offset}|${type}|${record.destination}|${record.messageAck.lastMessageId}";
	private String transactionFormat = "${location.dataFileId},${location.offset}|${type}|${record.transactionId}";
	private String traceFormat = "${location.dataFileId},${location.offset}|${type}|${record.message}";
	private String unknownFormat = "${location.dataFileId},${location.offset}|${type}|${record.class.name}";
	private String where;
	private VelocityContext context;
	private VelocityEngine velocity;
	private boolean help;

	public static void main(String[] args) throws Exception {
		AMQJournalTool consumerTool = new AMQJournalTool();
		String[] directories = CommandLineSupport
				.setOptions(consumerTool, args);
		if (directories.length < 1) {
			System.out
					.println("Please specify the directories with journal data to scan");
			return;
		}
		for (int i = 0; i < directories.length; i++) {
			consumerTool.getDirs().add(new File(directories[i]));
		}
		consumerTool.execute();
	}

	public void execute() throws Exception {

		if( help ) {
			showHelp();
			return;
		}
		
		if (getDirs().size() < 1) {
			System.out.println("");
			System.out.println("Invalid Usage: Please specify the directories with journal data to scan");
			System.out.println("");
			showHelp();
			return;
		}

		for (File dir : getDirs()) {
			if( !dir.exists() ) {
				System.out.println("");
				System.out.println("Invalid Usage: the directory '"+dir.getPath()+"' does not exist");
				System.out.println("");
				showHelp();
				return;
			}
			if( !dir.isDirectory() ) {
				System.out.println("");
				System.out.println("Invalid Usage: the argument '"+dir.getPath()+"' is not a directory");
				System.out.println("");
				showHelp();
				return;
			}
		}
		
		
		context = new VelocityContext();
		List keys = Arrays.asList(context.getKeys());

		for (Iterator iterator = System.getProperties().entrySet()
				.iterator(); iterator.hasNext();) {
			Map.Entry kv = (Map.Entry) iterator.next();
			String name = (String) kv.getKey();
			String value = (String) kv.getValue();

			if (!keys.contains(name)) {
				context.put(name, value);
			}
		}
		
		velocity = new VelocityEngine();
		velocity.setProperty(Velocity.RESOURCE_LOADER, "all");
		velocity.setProperty("all.resource.loader.class", CustomResourceLoader.class.getName());
		velocity.init();


		resources.put("message", messageFormat);
		resources.put("topicAck", topicAckFormat);
		resources.put("queueAck", queueAckFormat);
		resources.put("transaction", transactionFormat);
		resources.put("trace", traceFormat);
		resources.put("unknown", unknownFormat);

		Query query = null;
		if (where != null) {
			query = new Query();
			query.parse("select * from "+Entry.class.getName()+" where "+where);

		}

		ReadOnlyAsyncDataManager manager = new ReadOnlyAsyncDataManager(getDirs());
		manager.start();
		try {
			Location curr = manager.getFirstLocation();
			while (curr != null) {

				ByteSequence data = manager.read(curr);
				DataStructure c = (DataStructure) wireFormat.unmarshal(data);

				Entry entry = new Entry();
				entry.setLocation(curr);
				entry.setRecord(c);
				entry.setData(data);
				entry.setQuery(query);
				process(entry);

				curr = manager.getNextLocation(curr);
			}
		} finally {
			manager.close();
		}
	}

	private void showHelp() {
		InputStream is = AMQJournalTool.class.getResourceAsStream("help.txt");
		Scanner scanner = new Scanner(is);
		while (scanner.hasNextLine()) {
			String line = scanner.nextLine();
			System.out.println(line);
		}
		scanner.close();	}

	private void process(Entry entry) throws Exception {

		Location location = entry.getLocation();
		DataStructure record = entry.getRecord();

		switch (record.getDataStructureType()) {
		case ActiveMQMessage.DATA_STRUCTURE_TYPE:
			entry.setType("ActiveMQMessage");
			entry.setFormater("message");
			display(entry);
			break;
		case ActiveMQBytesMessage.DATA_STRUCTURE_TYPE:
			entry.setType("ActiveMQBytesMessage");
			entry.setFormater("message");
			display(entry);
			break;
		case ActiveMQBlobMessage.DATA_STRUCTURE_TYPE:
			entry.setType("ActiveMQBlobMessage");
			entry.setFormater("message");
			display(entry);
			break;
		case ActiveMQMapMessage.DATA_STRUCTURE_TYPE:
			entry.setType("ActiveMQMapMessage");
			entry.setFormater("message");
			display(entry);
			break;
		case ActiveMQObjectMessage.DATA_STRUCTURE_TYPE:
			entry.setType("ActiveMQObjectMessage");
			entry.setFormater("message");
			display(entry);
			break;
		case ActiveMQStreamMessage.DATA_STRUCTURE_TYPE:
			entry.setType("ActiveMQStreamMessage");
			entry.setFormater("message");
			display(entry);
			break;
		case ActiveMQTextMessage.DATA_STRUCTURE_TYPE:
			entry.setType("ActiveMQTextMessage");
			entry.setFormater("message");
			display(entry);
			break;
		case JournalQueueAck.DATA_STRUCTURE_TYPE:
			entry.setType("Queue Ack");
			entry.setFormater("queueAck");
			display(entry);
			break;
		case JournalTopicAck.DATA_STRUCTURE_TYPE:
			entry.setType("Topic Ack");
			entry.setFormater("topicAck");
			display(entry);
			break;
		case JournalTransaction.DATA_STRUCTURE_TYPE:
			entry.setType(getType((JournalTransaction) record));
			entry.setFormater("transaction");
			display(entry);
			break;
		case JournalTrace.DATA_STRUCTURE_TYPE:
			entry.setType("Trace");
			entry.setFormater("trace");
			display(entry);
			break;
		default:
			entry.setType("Unknown");
			entry.setFormater("unknown");
			display(entry);
			break;
		}
	}

	private String getType(JournalTransaction record) {
		switch (record.getType()) {
		case JournalTransaction.XA_PREPARE:
			return "XA Prepare";
		case JournalTransaction.XA_COMMIT:
			return "XA Commit";
		case JournalTransaction.XA_ROLLBACK:
			return "XA Rollback";
		case JournalTransaction.LOCAL_COMMIT:
			return "Commit";
		case JournalTransaction.LOCAL_ROLLBACK:
			return "Rollback";
		}
		return "Unknown Transaction";
	}

	private void display(Entry entry) throws Exception {

		if (entry.getQuery() != null) {
			List list = Collections.singletonList(entry);
			List results = entry.getQuery().execute(list).getResults();
			if (results.isEmpty()) {
				return;
			}
		}

		CustomResourceLoader.setResources(resources);
		try {

			context.put("location", entry.getLocation());
			context.put("record", entry.getRecord());
			context.put("type", entry.getType());
			if (entry.getRecord() instanceof ActiveMQMessage) {
				context.put("body", new MessageBodyFormatter(
						(ActiveMQMessage) entry.getRecord()));
			}

			Template template = velocity.getTemplate(entry.getFormater());
			PrintWriter writer = new PrintWriter(System.out);
			template.merge(context, writer);
			writer.println();
			writer.flush();
		} finally {
			CustomResourceLoader.setResources(null);
		}
	}

	public void setMessageFormat(String messageFormat) {
		this.messageFormat = messageFormat;
	}

	public void setTopicAckFormat(String ackFormat) {
		this.topicAckFormat = ackFormat;
	}

	public void setTransactionFormat(String transactionFormat) {
		this.transactionFormat = transactionFormat;
	}

	public void setTraceFormat(String traceFormat) {
		this.traceFormat = traceFormat;
	}

	public void setUnknownFormat(String unknownFormat) {
		this.unknownFormat = unknownFormat;
	}

	public void setQueueAckFormat(String queueAckFormat) {
		this.queueAckFormat = queueAckFormat;
	}

	public String getQuery() {
		return where;
	}

	public void setWhere(String query) {
		this.where = query;
	}

	public boolean isHelp() {
		return help;
	}

	public void setHelp(boolean help) {
		this.help = help;
	}

	/**
	 * @return the dirs
	 */
	public ArrayList<File> getDirs() {
		return dirs;
	}

}
