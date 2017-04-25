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
package org.apache.activemq.transport.protocol;

import java.nio.ByteBuffer;

/**
 *
 *
 */
public class MqttProtocolVerifier implements ProtocolVerifier {

    @Override
    public boolean isProtocol(byte[] value) {
       ByteBuffer buf = ByteBuffer.wrap(value);

       if (!(buf.get() == 16 && validateRemainingLength(buf) && buf.get() == (byte) 0)) {
           return false;
       }
       byte b = buf.get() ;
       return ((b == 4 || b == 6) && (buf.get() == 77));
    }

    private boolean validateRemainingLength(ByteBuffer buffer) {
       byte msb = (byte) 0b10000000;
       for (byte i = 0; i < 4; i++) {
          if ((buffer.get() & msb) != msb) {
             return true;
          }
       }
       return false;
    }
}
