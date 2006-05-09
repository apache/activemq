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
import java.io.DataOutput;
import java.io.IOException;
/**
 * A an Item with a relative postion and location to other Items in the Store
 * 
 * @version $Revision: 1.2 $
 */
final class IndexItem implements Item{
    
    static final int INDEX_SIZE=43;
    //used by linked list
    IndexItem next;
    IndexItem prev;
    
    private long offset=POSITION_NOT_SET;
    private long previousItem=POSITION_NOT_SET;
    private long nextItem=POSITION_NOT_SET;
    private long keyOffset=POSITION_NOT_SET;
    private int keyFile=(int) POSITION_NOT_SET;
    private long valueOffset=POSITION_NOT_SET;
    private int valueFile=(int) POSITION_NOT_SET;
    private boolean active=true;
    

    /**
     * Default Constructor
     */
    IndexItem(){}

    void reset(){
        previousItem=POSITION_NOT_SET;
        nextItem=POSITION_NOT_SET;
        keyOffset=POSITION_NOT_SET;
        keyFile=(int) POSITION_NOT_SET;
        valueOffset=POSITION_NOT_SET;
        valueFile=(int) POSITION_NOT_SET;
        active=true;
    }

    DataItem getKeyDataItem(){
        DataItem result=new DataItem();
        result.setOffset(keyOffset);
        result.setFile(keyFile);
        return result;
    }

    DataItem getValueDataItem(){
        DataItem result=new DataItem();
        result.setOffset(valueOffset);
        result.setFile(valueFile);
        return result;
    }

    void setValueData(DataItem item){
        valueOffset=item.getOffset();
        valueFile=item.getFile();
    }

    void setKeyData(DataItem item){
        keyOffset=item.getOffset();
        keyFile=item.getFile();
    }

    /**
     * @param dataOut
     * @throws IOException
     */
    void write(DataOutput dataOut) throws IOException{
        dataOut.writeShort(MAGIC);
        dataOut.writeBoolean(active);
        dataOut.writeLong(previousItem);
        dataOut.writeLong(nextItem);
        dataOut.writeInt(keyFile);
        dataOut.writeLong(keyOffset);
        dataOut.writeInt(valueFile);
        dataOut.writeLong(valueOffset);
    }

    /**
     * @param dataIn
     * @throws IOException
     */
    void read(DataInput dataIn) throws IOException{
        if(dataIn.readShort()!=MAGIC){
            throw new BadMagicException();
        }
        active=dataIn.readBoolean();
        previousItem=dataIn.readLong();
        nextItem=dataIn.readLong();
        keyFile=dataIn.readInt();
        keyOffset=dataIn.readLong();
        valueFile=dataIn.readInt();
        valueOffset=dataIn.readLong();
    }

    /**
     * @param newPrevEntry
     */
    void setPreviousItem(long newPrevEntry){
        previousItem=newPrevEntry;
    }

    /**
     * @return prev item
     */
    long getPreviousItem(){
        return previousItem;
    }

    /**
     * @param newNextEntry
     */
    void setNextItem(long newNextEntry){
        nextItem=newNextEntry;
    }

    /**
     * @return next item
     */
    long getNextItem(){
        return nextItem;
    }

    /**
     * @param newObjectOffset
     */
    void setKeyOffset(long newObjectOffset){
        keyOffset=newObjectOffset;
    }

    /**
     * @return key offset
     */
    long getKeyOffset(){
        return keyOffset;
    }

    /**
     * @return Returns the keyFile.
     */
    int getKeyFile(){
        return keyFile;
    }

    /**
     * @param keyFile The keyFile to set.
     */
    void setKeyFile(int keyFile){
        this.keyFile=keyFile;
    }

    /**
     * @return Returns the valueFile.
     */
    int getValueFile(){
        return valueFile;
    }

    /**
     * @param valueFile The valueFile to set.
     */
    void setValueFile(int valueFile){
        this.valueFile=valueFile;
    }

    /**
     * @return Returns the valueOffset.
     */
    long getValueOffset(){
        return valueOffset;
    }

    /**
     * @param valueOffset The valueOffset to set.
     */
    void setValueOffset(long valueOffset){
        this.valueOffset=valueOffset;
    }

    /**
     * @return Returns the active.
     */
    boolean isActive(){
        return active;
    }

    /**
     * @param active The active to set.
     */
    void setActive(boolean active){
        this.active=active;
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
     * @return eprtty print of 'this'
     */
    public String toString(){
        String result="offset="+offset+" , keyFile = "+keyFile+" , keyOffset = "+keyOffset+", valueOffset = "
                        +valueOffset+" , previousItem = "+previousItem+" , nextItem = "+nextItem;
        return result;
    }
}