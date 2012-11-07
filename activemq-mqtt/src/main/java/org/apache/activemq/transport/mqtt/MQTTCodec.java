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
package org.apache.activemq.transport.mqtt;

import java.io.IOException;

import javax.jms.JMSException;
import org.apache.activemq.transport.tcp.TcpTransport;
import org.fusesource.hawtbuf.DataByteArrayInputStream;
import org.fusesource.hawtbuf.DataByteArrayOutputStream;
import org.fusesource.mqtt.codec.*;

public class MQTTCodec {

    TcpTransport transport;

    DataByteArrayOutputStream currentCommand = new DataByteArrayOutputStream();
    boolean processedHeader = false;
    String action;
    byte header;
    int contentLength = -1;
    int previousByte = -1;
    int payLoadRead = 0;

    public MQTTCodec(TcpTransport transport) {
        this.transport = transport;
    }

    public void parse(DataByteArrayInputStream input, int readSize) throws Exception {
        int i = 0;
        byte b;
        while (i++ < readSize) {
            b = input.readByte();
            // skip repeating nulls
            if (!processedHeader && b == 0) {
                previousByte = 0;
                continue;
            }

            if (!processedHeader) {
                i += processHeader(b, input);
                if (contentLength == 0) {
                    processCommand();
                }

            } else {

                if (contentLength == -1) {
                    // end of command reached, unmarshal
                    if (b == 0) {
                        processCommand();
                    } else {
                        currentCommand.write(b);
                    }
                } else {
                    // read desired content length
                    if (payLoadRead == contentLength) {
                        processCommand();
                        i += processHeader(b, input);
                    } else {
                        currentCommand.write(b);
                        payLoadRead++;
                    }
                }
            }

            previousByte = b;
        }
        if (processedHeader && payLoadRead == contentLength) {
            processCommand();
        }
    }

    /**
     * sets the content length
     *
     * @return number of bytes read
     */
    private int processHeader(byte header, DataByteArrayInputStream input) {
        this.header = header;
        byte digit;
        int multiplier = 1;
        int read = 0;
        int length = 0;
        do {
            digit = input.readByte();
            length += (digit & 0x7F) * multiplier;
            multiplier <<= 7;
            read++;
        } while ((digit & 0x80) != 0);

        contentLength = length;
        processedHeader = true;
        return read;
    }


    private void processCommand() throws Exception {
        MQTTFrame frame = new MQTTFrame(currentCommand.toBuffer().deepCopy()).header(header);
        transport.doConsume(frame);
        processedHeader = false;
        currentCommand.reset();
        contentLength = -1;
        payLoadRead = 0;
    }

    public static String commandType(byte header) throws IOException, JMSException {

        byte messageType = (byte) ((header & 0xF0) >>> 4);
        switch (messageType) {
            case PINGREQ.TYPE: {
                return "PINGREQ";
            }
            case CONNECT.TYPE: {
                return "CONNECT";
            }
            case DISCONNECT.TYPE: {
                return "DISCONNECT";
            }
            case SUBSCRIBE.TYPE: {
                return "SUBSCRIBE";
            }
            case UNSUBSCRIBE.TYPE: {
                return "UNSUBSCRIBE";
            }
            case PUBLISH.TYPE: {
                return "PUBLISH";
            }
            case PUBACK.TYPE: {
                return "PUBACK";
            }
            case PUBREC.TYPE: {
                return "PUBREC";
            }
            case PUBREL.TYPE: {
                return "PUBREL";
            }
            case PUBCOMP.TYPE: {
                return "PUBCOMP";
            }
            default:
                return "UNKNOWN";
        }

    }

}
