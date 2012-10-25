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
package org.apache.activemq.transport.amqp.transform;

import org.apache.qpid.proton.message.Message;
import org.apache.qpid.proton.type.Binary;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class EncodedMessage extends Binary {

    final long messageFormat;

    public EncodedMessage(long messageFormat, byte[] data, int offset, int length) {
        super(data, offset, length);
        this.messageFormat = messageFormat;
    }

    public long getMessageFormat() {
        return messageFormat;
    }

    public Message decode() throws Exception {
        Message amqp = new Message();

        int offset = getArrayOffset();
        int len = getLength();
        while( len > 0 ) {
            final int decoded = amqp.decode(getArray(), offset, len);
            assert decoded > 0: "Make progress decoding the message";
            offset += decoded;
            len -= decoded;
        }

        return amqp;
    }
}
