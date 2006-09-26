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
    private byte[] header=new byte[DataItem.HEAD_SIZE];

    /**
     * Construct a Store reader
     * 
     * @param file
     */
    StoreDataReader(DataManager fileManager){
        this.dataManager=fileManager;
        this.dataIn=new StoreByteArrayInputStream();
    }

    protected Object readItem(Marshaller marshaller,DataItem item) throws IOException{
        RandomAccessFile file=dataManager.getDataFile(item);
        file.seek(item.getOffset());
        file.readFully(header);
        dataIn.restart(header);
        item.readHeader(dataIn);
        byte[] data=new byte[item.getSize()];
        file.readFully(data);
        dataIn.restart(data);
        return marshaller.readPayload(dataIn);
    }
}
