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
import org.apache.activemq.command.MessageId;
import org.apache.activemq.kaha.Marshaller;
import org.apache.activemq.kaha.impl.index.IndexItem;


/**
 * Marshall a TopicSubAck
 * @version $Revision: 1.10 $
 */
public class ConsumerMessageRefMarshaller implements Marshaller{
   

    /**
     * @param object
     * @param dataOut
     * @throws IOException
     * @see org.apache.activemq.kaha.Marshaller#writePayload(java.lang.Object, java.io.DataOutput)
     */
    public void writePayload(Object object,DataOutput dataOut) throws IOException{
       ConsumerMessageRef ref = (ConsumerMessageRef) object;
       dataOut.writeUTF(ref.getMessageId().toString());
       IndexItem item = (IndexItem)ref.getMessageEntry();
       dataOut.writeLong(item.getOffset());
       item.write(dataOut);
       item = (IndexItem)ref.getAckEntry();
       dataOut.writeLong(item.getOffset());
       item.write(dataOut);
       
       
    }

    /**
     * @param dataIn
     * @return payload
     * @throws IOException
     * @see org.apache.activemq.kaha.Marshaller#readPayload(java.io.DataInput)
     */
    public Object readPayload(DataInput dataIn) throws IOException{
        ConsumerMessageRef ref = new ConsumerMessageRef();
        ref.setMessageId(new MessageId(dataIn.readUTF()));
        IndexItem item = new IndexItem();
        item.setOffset(dataIn.readLong());
        item.read(dataIn);
        ref.setMessageEntry(item);
        item = new IndexItem();
        item.setOffset(dataIn.readLong());
        item.read(dataIn);
        ref.setAckEntry(item);
        return ref;
    }
}
