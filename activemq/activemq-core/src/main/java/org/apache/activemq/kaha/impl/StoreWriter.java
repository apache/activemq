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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import org.apache.activemq.kaha.Marshaller;

/**
 * Optimized Store writer
 * 
 * @version $Revision: 1.1.1.1 $
 */
class StoreWriter{
    protected RandomAccessFile dataFile;
    protected StoreByteArrayOutputStream bytesOut;
    protected DataOutputStream dataOut;
    
    /**
     * Construct a Store writer
     * @param file
     */
    StoreWriter(RandomAccessFile file){
        this.dataFile = file;
        this.bytesOut = new StoreByteArrayOutputStream();
        this.dataOut = new DataOutputStream(bytesOut);
    }
    
    void updateHeader(Item item) throws IOException{
        bytesOut.reset();
        item.writeHeader(dataOut);
        dataFile.seek(item.getOffset());
        dataFile.write(bytesOut.getData(),0,bytesOut.size());
    }
    
    void updatePayload(Item item) throws IOException{
        bytesOut.reset();
        dataFile.seek(item.getOffset() + Item.HEAD_SIZE);
        item.writeLocation(dataOut);
        dataFile.write(bytesOut.getData(),0,bytesOut.size());
    }
    
    int loadPayload(Marshaller marshaller, Object payload,Item item) throws IOException{
        bytesOut.reset();
        bytesOut.position(Item.HEAD_SIZE);
        item.writePayload(marshaller, payload, dataOut);
        return bytesOut.size() - Item.HEAD_SIZE;
    }
    
    void storeItem(Item item,int payloadSize) throws IOException{
        bytesOut.reset();
        item.writeHeader(dataOut);
        dataFile.seek(item.getOffset());
        dataFile.write(bytesOut.getData(),0,payloadSize+Item.HEAD_SIZE);
        
    }
    
   
    
    void writeShort(long offset, int value) throws IOException{
        bytesOut.reset();
        dataFile.seek(offset);
        dataOut.writeShort(value);    
        dataFile.write(bytesOut.getData(),0,bytesOut.size());
    }
    
    void writeInt(long offset,int value) throws IOException{
        bytesOut.reset();
        dataFile.seek(offset);
        dataOut.writeInt(value);    
        dataFile.write(bytesOut.getData(),0,bytesOut.size());
    }
    
    void writeLong(long offset,long value) throws IOException{
        bytesOut.reset();
        dataFile.seek(offset);
        dataOut.writeLong(value);    
        dataFile.write(bytesOut.getData(),0,bytesOut.size());
    }
    
    long length() throws IOException{
        return dataFile.length();
    }
    
    long position() throws IOException{
        return dataFile.getFilePointer();
    }
    
    void position(long newPosition) throws IOException{
        dataFile.seek(newPosition);
    }
    
    void allocateSpace(long newLength) throws IOException{
        dataFile.getFD().sync();
        long currentOffset=dataFile.getFilePointer();
        dataFile.seek(newLength);
        dataFile.write(0);
        dataFile.seek(currentOffset);
        dataFile.getFD().sync();
    }
}
