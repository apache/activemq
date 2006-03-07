/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.transport.udp;


import java.nio.ByteBuffer;

/**
 * 
 * @version $Revision$
 */
public class DatagramHeaderMarshaller {

    public DatagramHeader readHeader(ByteBuffer readBuffer) {
        DatagramHeader answer = new DatagramHeader();
        answer.setCounter(readBuffer.getLong());
        byte flags = readBuffer.get();
        answer.setFlags(flags);
        return answer;
    }

    public void writeHeader(DatagramHeader header, ByteBuffer writeBuffer) {
        writeBuffer.putLong(header.getCounter());
        writeBuffer.put(header.getFlags());
    }

    public int getHeaderSize(DatagramHeader header) {
        return 8 + 1;
    }

}
