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
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.util.HashMap;
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

    private boolean encodingEnabled = false;
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

        if (stomp.getAction().equals(Stomp.Commands.KEEPALIVE)) {
            os.write(Stomp.BREAK);
            return;
        }

        StringBuilder buffer = new StringBuilder();
        buffer.append(stomp.getAction());
        buffer.append(Stomp.NEWLINE);

        // Output the headers.
        for (Map.Entry<String, String> entry : stomp.getHeaders().entrySet()) {
            buffer.append(entry.getKey());
            buffer.append(Stomp.Headers.SEPERATOR);
            buffer.append(encodeHeader(entry.getValue()));
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

            // parse action
            String action = parseAction(in);

            // Parse the headers
            HashMap<String, String> headers = parseHeaders(in);

            // Read in the data part.
            byte[] data = NO_DATA;
            String contentLength = headers.get(Stomp.Headers.CONTENT_LENGTH);
            if (contentLength != null) {

                // Bless the client, he's telling us how much data to read in.
                int length = parseContentLength(contentLength);

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
        ByteSequence sequence = readHeaderLine(in, maxLength, errorMessage);
        return new String(sequence.getData(), sequence.getOffset(), sequence.getLength(), "UTF-8").trim();
    }

    private ByteSequence readHeaderLine(DataInput in, int maxLength, String errorMessage) throws IOException {
        byte b;
        ByteArrayOutputStream baos = new ByteArrayOutputStream(maxLength);
        while ((b = in.readByte()) != '\n') {
            if (baos.size() > maxLength) {
                throw new ProtocolException(errorMessage, true);
            }
            baos.write(b);
        }
        baos.close();
        return baos.toByteSequence();
    }

    protected String parseAction(DataInput in) throws IOException {
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
        return action;
    }

    protected HashMap<String, String> parseHeaders(DataInput in) throws IOException {
        HashMap<String, String> headers = new HashMap<String, String>(25);
        while (true) {
            ByteSequence line = readHeaderLine(in, MAX_HEADER_LENGTH, "The maximum header length was exceeded");
            if (line != null && line.length > 0) {

                if (headers.size() > MAX_HEADERS) {
                    throw new ProtocolException("The maximum number of headers was exceeded", true);
                }

                try {

                    ByteArrayInputStream headerLine = new ByteArrayInputStream(line);
                    ByteArrayOutputStream stream = new ByteArrayOutputStream(line.length);

                    // First complete the name
                    int result = -1;
                    while ((result = headerLine.read()) != -1) {
                        if (result != ':') {
                            stream.write(result);
                        } else {
                            break;
                        }
                    }

                    ByteSequence nameSeq = stream.toByteSequence();
                    String name = new String(nameSeq.getData(), nameSeq.getOffset(), nameSeq.getLength(), "UTF-8").trim();
                    String value = decodeHeader(headerLine).trim();
                    headers.put(name, value);
                } catch (Exception e) {
                    throw new ProtocolException("Unable to parser header line [" + line + "]", true);
                }
            } else {
                break;
            }
        }
        return headers;
    }

    protected int parseContentLength(String contentLength) throws ProtocolException {
        int length;
        try {
            length = Integer.parseInt(contentLength.trim());
        } catch (NumberFormatException e) {
            throw new ProtocolException("Specified content-length is not a valid integer", true);
        }

        if (length > MAX_DATA_LENGTH) {
            throw new ProtocolException("The maximum data length was exceeded", true);
        }

        return length;
    }

    private String encodeHeader(String header) throws IOException {
        String result = header;
        if (this.encodingEnabled) {
            byte[] utf8buf = header.getBytes("UTF-8");
            ByteArrayOutputStream stream = new ByteArrayOutputStream(utf8buf.length);
            for(byte val : utf8buf) {
                switch(val) {
                case Stomp.ESCAPE:
                    stream.write(Stomp.ESCAPE_ESCAPE_SEQ);
                    break;
                case Stomp.BREAK:
                    stream.write(Stomp.NEWLINE_ESCAPE_SEQ);
                    break;
                case Stomp.COLON:
                    stream.write(Stomp.COLON_ESCAPE_SEQ);
                    break;
                default:
                    stream.write(val);
                }
            }
            result =  new String(stream.toByteArray(), "UTF-8");
        }

        return result;
    }

    private String decodeHeader(InputStream header) throws IOException {
        ByteArrayOutputStream decoded = new ByteArrayOutputStream();
        PushbackInputStream stream = new PushbackInputStream(header);

        int value = -1;
        while( (value = stream.read()) != -1) {
            if (value == 92) {

                int next = stream.read();
                if (next != -1) {
                    switch(next) {
                    case 110:
                        decoded.write(Stomp.BREAK);
                        break;
                    case 99:
                        decoded.write(Stomp.COLON);
                        break;
                    case 92:
                        decoded.write(Stomp.ESCAPE);
                        break;
                    default:
                        stream.unread(next);
                        decoded.write(value);
                    }
                } else {
                    decoded.write(value);
                }

            } else {
                decoded.write(value);
            }
        }

        return new String(decoded.toByteArray(), "UTF-8");
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public boolean isEncodingEnabled() {
        return this.encodingEnabled;
    }

    public void setEncodingEnabled(boolean value) {
        this.encodingEnabled = value;
    }

}
