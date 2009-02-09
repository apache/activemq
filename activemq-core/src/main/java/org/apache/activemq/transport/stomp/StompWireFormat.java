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
package org.apache.activemq.transport.stomp;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.activemq.util.ByteArrayInputStream;
import org.apache.activemq.util.ByteArrayOutputStream;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.wireformat.WireFormat;

/**
 * Implements marshalling and unmarsalling the <a
 * href="http://stomp.codehaus.org/">Stomp</a> protocol.
 */
public class StompWireFormat implements WireFormat {

    private static final byte[] NO_DATA = new byte[] {};
    private static final byte[] END_OF_FRAME = new byte[] {0, '\n'};

    private static final int MAX_COMMAND_LENGTH = 1024;
    private static final int MAX_HEADER_LENGTH = 1024 * 10;
    private static final int MAX_HEADERS = 1000;
    private static final int MAX_DATA_LENGTH = 1024 * 1024 * 100;

    private int version = 1;

    public ByteSequence marshal(Object command) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        marshal(command, dos);
        dos.close();
        return baos.toByteSequence();
    }

    public Object unmarshal(ByteSequence packet) throws IOException {
        ByteArrayInputStream stream = new ByteArrayInputStream(packet);
        DataInputStream dis = new DataInputStream(stream);
        return unmarshal(dis);
    }

    public void marshal(Object command, DataOutput os) throws IOException {
        StompFrame stomp = (org.apache.activemq.transport.stomp.StompFrame)command;

        StringBuffer buffer = new StringBuffer();
        buffer.append(stomp.getAction());
        buffer.append(Stomp.NEWLINE);

        // Output the headers.
        for (Iterator iter = stomp.getHeaders().entrySet().iterator(); iter.hasNext();) {
            Map.Entry entry = (Map.Entry)iter.next();
            buffer.append(entry.getKey());
            buffer.append(Stomp.Headers.SEPERATOR);
            buffer.append(entry.getValue());
            buffer.append(Stomp.NEWLINE);
        }

        // Add a newline to seperate the headers from the content.
        buffer.append(Stomp.NEWLINE);

        os.write(buffer.toString().getBytes("UTF-8"));
        os.write(stomp.getContent());
        os.write(END_OF_FRAME);
    }

    public Object unmarshal(DataInput in) throws IOException {

        try {
            String action = null;

            // skip white space to next real action line
            while (true) {
                action = readLine(in, MAX_COMMAND_LENGTH, "The maximum command length was exceeded");
                if (action == null) {
                    throw new IOException("connection was closed");
                } else {
                    action = action.trim();
                    if (action.length() > 0) {
                        break;
                    }
                }
            }

            // Parse the headers
            HashMap<String, String> headers = new HashMap<String, String>(25);
            while (true) {
                String line = readLine(in, MAX_HEADER_LENGTH, "The maximum header length was exceeded");
                if (line != null && line.trim().length() > 0) {

                    if (headers.size() > MAX_HEADERS) {
                        throw new ProtocolException("The maximum number of headers was exceeded", true);
                    }

                    try {
                        int seperatorIndex = line.indexOf(Stomp.Headers.SEPERATOR);
                        String name = line.substring(0, seperatorIndex).trim();
                        String value = line.substring(seperatorIndex + 1, line.length()).trim();
                        headers.put(name, value);
                    } catch (Exception e) {
                        throw new ProtocolException("Unable to parser header line [" + line + "]", true);
                    }
                } else {
                    break;
                }
            }

            // Read in the data part.
            byte[] data = NO_DATA;
            String contentLength = headers.get(Stomp.Headers.CONTENT_LENGTH);
            if (contentLength != null) {

                // Bless the client, he's telling us how much data to read in.
                int length;
                try {
                    length = Integer.parseInt(contentLength.trim());
                } catch (NumberFormatException e) {
                    throw new ProtocolException("Specified content-length is not a valid integer", true);
                }

                if (length > MAX_DATA_LENGTH) {
                    throw new ProtocolException("The maximum data length was exceeded", true);
                }

                data = new byte[length];
                in.readFully(data);

                if (in.readByte() != 0) {
                    throw new ProtocolException(Stomp.Headers.CONTENT_LENGTH + " bytes were read and " + "there was no trailing null byte", true);
                }

            } else {

                // We don't know how much to read.. data ends when we hit a 0
                byte b;
                ByteArrayOutputStream baos = null;
                while ((b = in.readByte()) != 0) {

                    if (baos == null) {
                        baos = new ByteArrayOutputStream();
                    } else if (baos.size() > MAX_DATA_LENGTH) {
                        throw new ProtocolException("The maximum data length was exceeded", true);
                    }

                    baos.write(b);
                }

                if (baos != null) {
                    baos.close();
                    data = baos.toByteArray();
                }

            }

            return new StompFrame(action, headers, data);

        } catch (ProtocolException e) {
            return new StompFrameError(e);
        }

    }

    private String readLine(DataInput in, int maxLength, String errorMessage) throws IOException {
        byte b;
        ByteArrayOutputStream baos = new ByteArrayOutputStream(maxLength);
        while ((b = in.readByte()) != '\n') {
            if (baos.size() > maxLength) {
                throw new ProtocolException(errorMessage, true);
            }
            baos.write(b);
        }
        baos.close();
        ByteSequence sequence = baos.toByteSequence();
        return new String(sequence.getData(), sequence.getOffset(), sequence.getLength(), "UTF-8");
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

	public boolean inReceive() {
		//TODO implement the inactivity monitor
		return false;
	}
    
    

}
