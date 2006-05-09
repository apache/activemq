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

import org.apache.activemq.kaha.Marshaller;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
/**
 * A a wrapper for a data in the store
 * 
 * @version $Revision: 1.2 $
 */
final class DataItem implements Item{
    static final int HEAD_SIZE=6; // magic + len
    private int size;
    private long offset=POSITION_NOT_SET;
    private int file=(int) POSITION_NOT_SET;

    DataItem(){}
    
    boolean isValid(){
        return file != POSITION_NOT_SET;
    }

    void writeHeader(DataOutput dataOut) throws IOException{
        dataOut.writeShort(MAGIC);
        dataOut.writeInt(size);
    }

    void readHeader(DataInput dataIn) throws IOException{
        int magic=dataIn.readShort();
        if(magic==MAGIC){
            size=dataIn.readInt();
        }else{
            throw new BadMagicException("Unexpected Magic value: "+magic);
        }
    }

    void writePayload(Marshaller marshaller,Object object,DataOutputStream dataOut) throws IOException{
        marshaller.writePayload(object,dataOut);
    }

    Object readPayload(Marshaller marshaller,DataInputStream dataIn) throws IOException{
        return marshaller.readPayload(dataIn);
    }

    /**
     * @return Returns the size.
     */
    int getSize(){
        return size;
    }

    /**
     * @param size The size to set.
     */
    void setSize(int size){
        this.size=size;
    }

    /**
     * @return Returns the offset.
     */
    long getOffset(){
        return offset;
    }

    /**
     * @param offset The offset to set.
     */
    void setOffset(long offset){
        this.offset=offset;
    }

    /**
     * @return Returns the file.
     */
    int getFile(){
        return file;
    }

    /**
     * @param file The file to set.
     */
    void setFile(int file){
        this.file=file;
    }

    /**
     * @return a pretty print
     */
    public String toString(){
        String result="offset = "+offset+", file = " + file + ", size = "+size;
        return result;
    }
}