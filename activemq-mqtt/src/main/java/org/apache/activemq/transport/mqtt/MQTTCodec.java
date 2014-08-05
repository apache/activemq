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

import org.apache.activemq.transport.tcp.TcpTransport;
import org.fusesource.hawtbuf.DataByteArrayInputStream;
import org.fusesource.hawtbuf.DataByteArrayOutputStream;
import org.fusesource.mqtt.codec.MQTTFrame;

public class MQTTCodec {

    private final MQTTFrameSink frameSink;
    private final DataByteArrayOutputStream currentCommand = new DataByteArrayOutputStream();
    private byte header;

    private int contentLength = -1;
    private int payLoadRead = 0;

    public interface MQTTFrameSink {
        void onFrame(MQTTFrame mqttFrame);
    }

    private FrameParser currentParser;

    // Internal parsers implement this and we switch to the next as we go.
    private interface FrameParser {

        void parse(DataByteArrayInputStream data, int readSize) throws IOException;

        void reset() throws IOException;
    }

    public MQTTCodec(MQTTFrameSink sink) {
        this.frameSink = sink;
    }

    public MQTTCodec(final TcpTransport transport) {
        this.frameSink = new MQTTFrameSink() {

            @Override
            public void onFrame(MQTTFrame mqttFrame) {
                transport.doConsume(mqttFrame);
            }
        };
    }

    public void parse(DataByteArrayInputStream input, int readSize) throws Exception {
        if (currentParser == null) {
            currentParser = initializeHeaderParser();
        }

        // Parser stack will run until current incoming data has all been consumed.
        currentParser.parse(input, readSize);
    }

    private void processCommand() throws IOException {
        MQTTFrame frame = new MQTTFrame(currentCommand.toBuffer().deepCopy()).header(header);
        frameSink.onFrame(frame);
    }

    //----- Prepare the current frame parser for use -------------------------//

    private FrameParser initializeHeaderParser() throws IOException {
        headerParser.reset();
        return headerParser;
    }

    private FrameParser initializeVariableLengthParser() throws IOException {
        variableLengthParser.reset();
        return variableLengthParser;
    }

    private FrameParser initializeContentParser() throws IOException {
        contentParser.reset();
        return contentParser;
    }

    //----- Frame parser implementations -------------------------------------//

    private final FrameParser headerParser = new FrameParser() {

        @Override
        public void parse(DataByteArrayInputStream data, int readSize) throws IOException {
            int i = 0;
            while (i++ < readSize) {
                byte b = data.readByte();
                // skip repeating nulls
                if (b == 0) {
                    continue;
                }

                header = b;

                currentParser = initializeVariableLengthParser();
                currentParser.parse(data, readSize - 1);
                return;
            }
        }

        @Override
        public void reset() throws IOException {
            header = -1;
        }
    };

    private final FrameParser contentParser = new FrameParser() {

        @Override
        public void parse(DataByteArrayInputStream data, int readSize) throws IOException {
            int i = 0;
            while (i++ < readSize) {
                currentCommand.write(data.readByte());
                payLoadRead++;

                if (payLoadRead == contentLength) {
                    processCommand();
                    currentParser = initializeHeaderParser();
                    currentParser.parse(data, readSize - i);
                    return;
                }
            }
        }

        @Override
        public void reset() throws IOException {
            contentLength = -1;
            payLoadRead = 0;
            currentCommand.reset();
        }
    };

    private final FrameParser variableLengthParser = new FrameParser() {

        private byte digit;
        private int multiplier = 1;
        private int length;

        @Override
        public void parse(DataByteArrayInputStream data, int readSize) throws IOException {
            int i = 0;
            while (i++ < readSize) {
                digit = data.readByte();
                length += (digit & 0x7F) * multiplier;
                multiplier <<= 7;
                if ((digit & 0x80) == 0) {
                    if (length == 0) {
                        processCommand();
                        currentParser = initializeHeaderParser();
                    } else {
                        currentParser = initializeContentParser();
                        contentLength = length;
                    }
                    currentParser.parse(data, readSize - i);
                    return;
                }
            }
        }

        @Override
        public void reset() throws IOException {
            digit = 0;
            multiplier = 1;
            length = 0;
        }
    };
}
