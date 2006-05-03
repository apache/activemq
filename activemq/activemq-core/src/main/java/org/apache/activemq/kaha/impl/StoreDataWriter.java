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
/**
 * Optimized writes to a RandomAcessFile
 * 
 * @version $Revision: 1.1.1.1 $
 */
package org.apache.activemq.kaha.impl;

import java.io.IOException;
import org.apache.activemq.kaha.Marshaller;
/**
 * Optimized Store writer
 * 
 * @version $Revision: 1.1.1.1 $
 */
final class StoreDataWriter{
    private StoreByteArrayOutputStream dataOut;
    private DataManager dataManager;

    /**
     * Construct a Store writer
     * 
     * @param file
     */
    StoreDataWriter(DataManager fileManager){
        this.dataManager=fileManager;
        this.dataOut=new StoreByteArrayOutputStream();
    }

    DataItem storeItem(Marshaller marshaller,Object payload) throws IOException{
        dataOut.reset();
        dataOut.position(DataItem.HEAD_SIZE);
        marshaller.writePayload(payload,dataOut);
        int size=dataOut.size();
        int payloadSize=size-DataItem.HEAD_SIZE;
        DataItem item=new DataItem();
        item.setSize(payloadSize);
        DataFile dataFile=dataManager.findSpaceForData(item);
        dataOut.reset();
        item.writeHeader(dataOut);
        dataFile.getRandomAccessFile().seek(item.getOffset());
        dataFile.getRandomAccessFile().write(dataOut.getData(),0,size);
        dataFile.incrementLength(size);
        dataManager.addInterestInFile(dataFile);
        return item;
    }
}
