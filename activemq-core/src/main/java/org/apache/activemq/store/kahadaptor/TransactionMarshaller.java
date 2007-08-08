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
import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.command.BaseCommand;
import org.apache.activemq.kaha.Marshaller;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.wireformat.WireFormat;

/**
 * Marshall a Transaction
 * @version $Revision: 1.10 $
 */
public class TransactionMarshaller implements Marshaller{
    
    private WireFormat wireFormat;
    public TransactionMarshaller(WireFormat wireFormat){
        this.wireFormat = wireFormat;
      
    }
    
    public void writePayload(Object object,DataOutput dataOut) throws IOException{
        KahaTransaction kt = (KahaTransaction) object;
        List list = kt.getList();
        dataOut.writeInt(list.size());
        for (int i = 0; i < list.size(); i++){
            TxCommand tx = (TxCommand) list.get(i);
            Object key = tx.getMessageStoreKey();
            ByteSequence packet = wireFormat.marshal(key);
            dataOut.writeInt(packet.length);
            dataOut.write(packet.data, packet.offset, packet.length);
            Object command = tx.getCommand();
            packet = wireFormat.marshal(command);
            dataOut.writeInt(packet.length);
            dataOut.write(packet.data, packet.offset, packet.length);
            
        }
       }

   
    public Object readPayload(DataInput dataIn) throws IOException{
        KahaTransaction result = new KahaTransaction();
        List list = new ArrayList();
        result.setList(list);
        int number=dataIn.readInt();
        for (int i = 0; i < number; i++){
            TxCommand command = new TxCommand();
            int size = dataIn.readInt();
            byte[] data=new byte[size];
            dataIn.readFully(data);
            Object key =  wireFormat.unmarshal(new ByteSequence(data));
            command.setMessageStoreKey(key);
            size = dataIn.readInt();
            data=new byte[size];
            dataIn.readFully(data);
            BaseCommand bc =  (BaseCommand) wireFormat.unmarshal(new ByteSequence(data));
            command.setCommand(bc);
            list.add(command);
        }
        return result;
       
    }
}
