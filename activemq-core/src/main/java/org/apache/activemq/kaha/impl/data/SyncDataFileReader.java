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
package org.apache.activemq.kaha.impl.data;

import java.io.IOException;
import java.io.RandomAccessFile;
import org.apache.activemq.kaha.Marshaller;
import org.apache.activemq.kaha.StoreLocation;
import org.apache.activemq.util.DataByteArrayInputStream;
/**
 * Optimized Store reader
 * 
 * @version $Revision: 1.1.1.1 $
 */
public final class SyncDataFileReader {
    
    private DataManagerImpl dataManager;
    private DataByteArrayInputStream dataIn;

    /**
     * Construct a Store reader
     * 
     * @param fileId
     */
    SyncDataFileReader(DataManagerImpl fileManager){
        this.dataManager=fileManager;
        this.dataIn=new DataByteArrayInputStream();
    }

    /* (non-Javadoc)
	 * @see org.apache.activemq.kaha.impl.data.DataFileReader#readDataItemSize(org.apache.activemq.kaha.impl.data.DataItem)
	 */
    public synchronized byte readDataItemSize(DataItem item) throws IOException {
        RandomAccessFile file = dataManager.getDataFile(item).getRandomAccessFile();
        file.seek(item.getOffset()); // jump to the size field
        byte rc = file.readByte();
        item.setSize(file.readInt());
        return rc;
    }
    
    /* (non-Javadoc)
	 * @see org.apache.activemq.kaha.impl.data.DataFileReader#readItem(org.apache.activemq.kaha.Marshaller, org.apache.activemq.kaha.StoreLocation)
	 */
    public synchronized  Object readItem(Marshaller marshaller,StoreLocation item) throws IOException{
        RandomAccessFile file=dataManager.getDataFile(item).getRandomAccessFile();
        
        // TODO: we could reuse the buffer in dataIn if it's big enough to avoid
        // allocating byte[] arrays on every readItem.
        byte[] data=new byte[item.getSize()];
        file.seek(item.getOffset()+DataManagerImpl.ITEM_HEAD_SIZE);
        file.readFully(data);
        dataIn.restart(data);
        return marshaller.readPayload(dataIn);
    }
}
