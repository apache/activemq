/**
 * 
 * Copyright 2005-2006 The Apache Software Foundation
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.activemq.kaha.impl;

import java.io.IOException;
import java.io.RandomAccessFile;
import org.apache.activemq.kaha.Marshaller;
/**
 * Optimized Store reader
 * 
 * @version $Revision: 1.1.1.1 $
 */
final class StoreDataReader{
    
    private DataManager dataManager;
    private StoreByteArrayInputStream dataIn;

    /**
     * Construct a Store reader
     * 
     * @param file
     */
    StoreDataReader(DataManager fileManager){
        this.dataManager=fileManager;
        this.dataIn=new StoreByteArrayInputStream();
    }

    /**
     * Sets the size property on a DataItem and returns the type of item that this was 
     * created as.
     * 
     * @param marshaller
     * @param item
     * @return
     * @throws IOException
     */
    protected byte readDataItemSize(DataItem item) throws IOException {

        RandomAccessFile file = dataManager.getDataFile(item);
        file.seek(item.getOffset()); // jump to the size field
        byte rc = file.readByte();
        item.setSize(file.readInt());
        return rc;
    }
    
    protected Object readItem(Marshaller marshaller,DataItem item) throws IOException{
        RandomAccessFile file=dataManager.getDataFile(item);
        
        // TODO: we could reuse the buffer in dataIn if it's big enough to avoid
        // allocating byte[] arrays on every readItem.
        byte[] data=new byte[item.getSize()];
        file.seek(item.getOffset()+DataManager.ITEM_HEAD_SIZE);
        file.readFully(data);
        dataIn.restart(data);
        return marshaller.readPayload(dataIn);
    }
}
