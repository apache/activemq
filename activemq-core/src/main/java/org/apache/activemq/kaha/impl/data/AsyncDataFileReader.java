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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;

import org.apache.activemq.kaha.Marshaller;
import org.apache.activemq.kaha.StoreLocation;
import org.apache.activemq.kaha.impl.data.AsyncDataFileWriter.WriteCommand;
import org.apache.activemq.kaha.impl.data.AsyncDataFileWriter.WriteKey;
import org.apache.activemq.util.ByteArrayInputStream;
import org.apache.activemq.util.DataByteArrayInputStream;
/**
 * Optimized Store reader
 * 
 * @version $Revision: 1.1.1.1 $
 */
final class AsyncDataFileReader implements DataFileReader {
    // static final Log log = LogFactory.getLog(AsyncDataFileReader.class);
    
    private DataManager dataManager;
    private DataByteArrayInputStream dataIn;
	private final Map inflightWrites;

    /**
     * Construct a Store reader
     * 
     * @param file
     */
    AsyncDataFileReader(DataManager fileManager, AsyncDataFileWriter writer){
        this.dataManager=fileManager;
		this.inflightWrites = writer.getInflightWrites();
        this.dataIn=new DataByteArrayInputStream();
    }

    /* (non-Javadoc)
	 * @see org.apache.activemq.kaha.impl.data.DataFileReader#readDataItemSize(org.apache.activemq.kaha.impl.data.DataItem)
	 */
    public synchronized byte readDataItemSize(DataItem item) throws IOException {
    	WriteCommand asyncWrite = (WriteCommand) inflightWrites.get(new WriteKey(item));
    	if( asyncWrite!= null ) {
    		item.setSize(asyncWrite.location.getSize());
    		return asyncWrite.data[0];
    	}
        RandomAccessFile file = dataManager.getDataFile(item).getRandomAccessFile();
        byte rc;
        synchronized(file) {
	        file.seek(item.getOffset()); // jump to the size field
	        rc = file.readByte();
	        item.setSize(file.readInt());
        }
        return rc;
    }
    
    /* (non-Javadoc)
	 * @see org.apache.activemq.kaha.impl.data.DataFileReader#readItem(org.apache.activemq.kaha.Marshaller, org.apache.activemq.kaha.StoreLocation)
	 */
    public synchronized Object readItem(Marshaller marshaller,StoreLocation item) throws IOException{
    	WriteCommand asyncWrite = (WriteCommand) inflightWrites.get(new WriteKey(item));
    	if( asyncWrite!= null ) {
            ByteArrayInputStream stream = new ByteArrayInputStream(asyncWrite.data, DataManager.ITEM_HEAD_SIZE, item.getSize());
            return marshaller.readPayload(new DataInputStream(stream));    		
    	}

    	RandomAccessFile file=dataManager.getDataFile(item).getRandomAccessFile();
        
        // TODO: we could reuse the buffer in dataIn if it's big enough to avoid
        // allocating byte[] arrays on every readItem.
		byte[] data=new byte[item.getSize()];
        synchronized(file) {
			file.seek(item.getOffset()+DataManager.ITEM_HEAD_SIZE);
			file.readFully(data);
        }
		dataIn.restart(data);
		return marshaller.readPayload(dataIn);
    }
}
