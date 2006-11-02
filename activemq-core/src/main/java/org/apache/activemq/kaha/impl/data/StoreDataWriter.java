/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.kaha.impl.data;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.apache.activemq.kaha.Marshaller;
import org.apache.activemq.kaha.StoreLocation;
import org.apache.activemq.util.DataByteArrayOutputStream;
/**
 * Optimized Store writer
 * 
 * @version $Revision: 1.1.1.1 $
 */
final class StoreDataWriter{
    
    private DataByteArrayOutputStream buffer;
    private DataManager dataManager;


    /**
     * Construct a Store writer
     * 
     * @param file
     */
    StoreDataWriter(DataManager fileManager){
        this.dataManager=fileManager;
        this.buffer=new DataByteArrayOutputStream();
    }

    /**
     * @param marshaller
     * @param payload
     * @param data_item2 
     * @return
     * @throws IOException
     * @throws FileNotFoundException
     */
    StoreLocation storeItem(Marshaller marshaller, Object payload, byte type) throws IOException {
        
        // Write the packet our internal buffer.
        buffer.reset();
        buffer.position(DataManager.ITEM_HEAD_SIZE);
        marshaller.writePayload(payload,buffer);
        int size=buffer.size();
        int payloadSize=size-DataManager.ITEM_HEAD_SIZE;
        buffer.reset();
        buffer.writeByte(type);
        buffer.writeInt(payloadSize);

        // Find the position where this item will land at.
        DataItem item=new DataItem();
        item.setSize(payloadSize);
        DataFile dataFile=dataManager.findSpaceForData(item);
        
        // Now splat the buffer to the file.
        dataFile.getRandomAccessFile().seek(item.getOffset());
        dataFile.getRandomAccessFile().write(buffer.getData(),0,size);
        dataFile.incrementLength(size);
        
        dataManager.addInterestInFile(dataFile);
        return item;
    }
    
    void updateItem(StoreLocation location,Marshaller marshaller, Object payload, byte type) throws IOException {
        //Write the packet our internal buffer.
        buffer.reset();
        buffer.position(DataManager.ITEM_HEAD_SIZE);
        marshaller.writePayload(payload,buffer);
        int size=buffer.size();
        int payloadSize=size-DataManager.ITEM_HEAD_SIZE;
        buffer.reset();
        buffer.writeByte(type);
        buffer.writeInt(payloadSize);
        RandomAccessFile  dataFile = dataManager.getDataFile(location);
        dataFile.seek(location.getOffset());
        dataFile.write(buffer.getData(),0,size);
    }
}
