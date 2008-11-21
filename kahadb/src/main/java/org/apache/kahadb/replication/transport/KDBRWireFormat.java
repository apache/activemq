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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.activemq.protobuf.InvalidProtocolBufferException;
import org.apache.activemq.protobuf.Message;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.kahadb.replication.ReplicationFrame;
import org.apache.kahadb.replication.pb.PBFileInfo;
import org.apache.kahadb.replication.pb.PBHeader;
import org.apache.kahadb.replication.pb.PBJournalLocation;
import org.apache.kahadb.replication.pb.PBJournalUpdate;
import org.apache.kahadb.replication.pb.PBSlaveInit;
import org.apache.kahadb.replication.pb.PBSlaveInitResponse;

public class KDBRWireFormat implements WireFormat {

	private int version;

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	public ByteSequence marshal(Object command) throws IOException {
		throw new RuntimeException("Not implemented.");
	}

	public Object unmarshal(ByteSequence packet) throws IOException {
		throw new RuntimeException("Not implemented.");
	}

	public void marshal(Object command, DataOutput out) throws IOException {
		OutputStream os = (OutputStream) out;
		ReplicationFrame frame = (ReplicationFrame) command;
		PBHeader header = frame.getHeader();
		switch (frame.getHeader().getType()) {
		case FILE_TRANSFER_RESPONSE: {
			// Write the header..
			header.writeFramed(os);
			// Stream the Payload.
			InputStream is = (InputStream) frame.getPayload();
			byte data[] = new byte[1024 * 4];
			int c;
			long remaining = frame.getHeader().getPayloadSize();
			while (remaining > 0 && (c = is.read(data, 0, (int) Math.min(remaining, data.length))) >= 0) {
				os.write(data, 0, c);
				remaining -= c;
			}
			break;
		}
		default:
			if (frame.getPayload() == null) {
				header.clearPayloadSize();
				header.writeFramed(os);
			} else {
				// All other payloads types are PB messages
				Message message = (Message) frame.getPayload();
				header.setPayloadSize(message.serializedSizeUnframed());
				header.writeFramed(os);
				message.writeUnframed(os);
			}
		}
	}

	public Object unmarshal(DataInput in) throws IOException {
		InputStream is = (InputStream) in;
		ReplicationFrame frame = new ReplicationFrame();
		frame.setHeader(PBHeader.parseFramed(is));
		switch (frame.getHeader().getType()) {
		case FILE_TRANSFER_RESPONSE:
			frame.setPayload(is);
			break;
		case FILE_TRANSFER:
			readPBPayload(frame, in, new PBFileInfo());
			break;
		case JOURNAL_UPDATE:
			readPBPayload(frame, in, new PBJournalUpdate());
			break;
		case JOURNAL_UPDATE_ACK:
			readPBPayload(frame, in, new PBJournalLocation());
			break;
		case SLAVE_INIT:
			readPBPayload(frame, in, new PBSlaveInit());
			break;
		case SLAVE_INIT_RESPONSE:
			readPBPayload(frame, in, new PBSlaveInitResponse());
			break;
		}
		return frame;
	}

	private void readPBPayload(ReplicationFrame frame, DataInput in, Message pb) throws IOException, InvalidProtocolBufferException {
		long payloadSize = frame.getHeader().getPayloadSize();
		byte[] payload;
		payload = new byte[(int)payloadSize];
		in.readFully(payload);
		frame.setPayload(pb.mergeUnframed(payload));
	}

}
