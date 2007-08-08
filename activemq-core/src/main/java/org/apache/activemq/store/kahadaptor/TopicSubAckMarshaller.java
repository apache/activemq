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
package org.apache.activemq.store.kahadaptor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.activemq.kaha.Marshaller;
import org.apache.activemq.kaha.impl.index.IndexItem;

/**
 * Marshall a TopicSubAck
 * 
 * @version $Revision: 1.10 $
 */
public class TopicSubAckMarshaller implements Marshaller {

    public void writePayload(Object object, DataOutput dataOut) throws IOException {
        TopicSubAck tsa = (TopicSubAck)object;
        dataOut.writeInt(tsa.getCount());
        IndexItem item = (IndexItem)tsa.getMessageEntry();
        dataOut.writeLong(item.getOffset());
        item.write(dataOut);

    }

    public Object readPayload(DataInput dataIn) throws IOException {
        TopicSubAck tsa = new TopicSubAck();
        int count = dataIn.readInt();
        tsa.setCount(count);
        IndexItem item = new IndexItem();
        item.setOffset(dataIn.readLong());
        item.read(dataIn);
        tsa.setMessageEntry(item);
        return tsa;
    }
}
