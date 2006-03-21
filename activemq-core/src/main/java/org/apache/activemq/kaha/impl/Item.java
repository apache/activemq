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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.activemq.kaha.Marshaller;
/**
 * A a wrapper for a data in the store
 * 
 * @version $Revision: 1.2 $
 */
public class Item{
    static final long POSITION_NOT_SET=-1;
    static final short MAGIC=31317;
    static final int ACTIVE=22;
    static final int FREE=33;
    static final int HEAD_SIZE=8; // magic + active + len
    static final int LOCATION_SIZE=24;
    private long offset=POSITION_NOT_SET;
    private int size;
    private boolean active;

    Item(){}

    void writeHeader(DataOutput dataOut) throws IOException{
        dataOut.writeShort(MAGIC);
        dataOut.writeByte(active?ACTIVE:FREE);
        dataOut.writeInt(size);
        dataOut.writeByte(0);//padding
    }

    void readHeader(DataInput dataIn) throws IOException{
        int magic=dataIn.readShort();
        if(magic==MAGIC){
            active=(dataIn.readByte()==ACTIVE);
            size=dataIn.readInt();
        }else if (magic == 0){
            size = -999; //end of data
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

    void readLocation(DataInput dataIn) throws IOException{}

    void writeLocation(DataOutput dataOut) throws IOException{}

    /**
     * @return Returns the size.
     */
    int getSize(){
        return size;
    }

    /**
     * @param size
     *            The size to set.
     */
    void setSize(int size){
        this.size=size;
    }

    void setOffset(long pos){
        offset=pos;
    }

    long getOffset(){
        return offset;
    }

    /**
     * @return Returns the active.
     */
    boolean isActive(){
        return active;
    }

    /**
     * @param active
     *            The active to set.
     */
    void setActive(boolean active){
        this.active=active;
    }

    /**
     * @return a pretty print
     */
    public String toString(){
        String result="offset = "+offset+" ,active = "+active+" , size = "+size;
        return result;
    }
}