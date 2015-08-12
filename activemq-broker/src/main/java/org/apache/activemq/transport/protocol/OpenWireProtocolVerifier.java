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

import org.apache.activemq.command.WireFormatInfo;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.openwire.OpenWireFormatFactory;

/**
 *
 *
 */
public class OpenWireProtocolVerifier  implements ProtocolVerifier {

    protected final OpenWireFormatFactory wireFormatFactory;

    public OpenWireProtocolVerifier(OpenWireFormatFactory wireFormatFactory) {
        this.wireFormatFactory = wireFormatFactory;
    }

    /* (non-Javadoc)
     * @see org.apache.activemq.broker.transport.protocol.ProtocolVerifier#isProtocol(byte[])
     */
    @Override
    public boolean isProtocol(byte[] value) {
        if (value.length < 8) {
           throw new IllegalArgumentException("Protocol header length changed "
                                                 + value.length);
        }

        int start = !((OpenWireFormat)wireFormatFactory.createWireFormat()).isSizePrefixDisabled() ? 4 : 0;
        int j = 0;
        // type
        if (value[start] != WireFormatInfo.DATA_STRUCTURE_TYPE) {
           return false;
        }
        start++;
        WireFormatInfo info = new WireFormatInfo();
        final byte[] magic = info.getMagic();
        int remainingLen = value.length - start;
        int useLen = remainingLen > magic.length ? magic.length : remainingLen;
        useLen += start;
        // magic
        for (int i = start; i < useLen; i++) {
           if (value[i] != magic[j]) {
              return false;
           }
           j++;
        }
        return true;
    }

}
