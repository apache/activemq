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
        answer.setDataSize(readBuffer.getInt());
        byte flags = readBuffer.get();
        answer.setFlags(flags);
        //System.out.println("Read header with counter: " + answer.getCounter() + "size: " + answer.getDataSize() + " with flags: " + flags);
        return answer;
    }

    public void writeHeader(DatagramHeader header, ByteBuffer writeBuffer) {
        writeBuffer.putLong(header.getCounter());
        writeBuffer.putInt(header.getDataSize());
        byte flags = header.getFlags();
        //System.out.println("Writing header with counter: " + header.getCounter() + " size: " + header.getDataSize() + " with flags: " + flags);
        writeBuffer.put(flags);
    }

    public int getHeaderSize(DatagramHeader header) {
        return 8 + 4 + 1;
    }

}
