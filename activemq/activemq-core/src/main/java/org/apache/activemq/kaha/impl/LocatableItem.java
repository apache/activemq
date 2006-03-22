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
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.activemq.kaha.Marshaller;

/**
* A an Item with a relative postion and location to other Items in the Store
* 
* @version $Revision: 1.2 $
*/
public final class LocatableItem extends Item implements Externalizable{
    
  
    private static final long serialVersionUID=-6888731361600185708L;
    private long previousItem=POSITION_NOT_SET;
    private long nextItem=POSITION_NOT_SET;
    private long referenceItem=POSITION_NOT_SET;
   

    public LocatableItem(){}

    public LocatableItem(long prev,long next,long objOffset) throws IOException{
        this.previousItem=prev;
        this.nextItem=next;
        this.referenceItem=objOffset;
    }


    public void writePayload(Marshaller marshaller,Object object,DataOutputStream dataOut) throws IOException{
        dataOut.writeLong(previousItem);
        dataOut.writeLong(nextItem);
        dataOut.writeLong(referenceItem);
        super.writePayload(marshaller,object,dataOut);
    }

    public Object readPayload(Marshaller marshaller,DataInputStream dataIn) throws IOException{
        previousItem=dataIn.readLong();
        nextItem=dataIn.readLong();
        referenceItem=dataIn.readLong();
        return super.readPayload(marshaller, dataIn);
    }
    
    void readLocation(DataInput dataIn) throws IOException{
        previousItem=dataIn.readLong();
        nextItem=dataIn.readLong();
        referenceItem=dataIn.readLong();
    }

    public void writeLocation(DataOutput dataOut) throws IOException{
        dataOut.writeLong(previousItem);
        dataOut.writeLong(nextItem);
    }

    public void setPreviousItem(long newPrevEntry){
        previousItem=newPrevEntry;
    }

    public long getPreviousItem(){
        return previousItem;
    }

    public void setNextItem(long newNextEntry){
        nextItem=newNextEntry;
    }

    public long getNextItem(){
        return nextItem;
    }

    public void setReferenceItem(long newObjectOffset){
        referenceItem=newObjectOffset;
    }

    public long getReferenceItem(){
        return referenceItem;
    }

    /* (non-Javadoc)
     * @see org.apache.activemq.kaha.impl.Item#toString()
     */
    public String toString(){
        String result=super.toString();
        result+=" , referenceItem = "+referenceItem+", previousItem = "+previousItem+" , nextItem = "+nextItem;
        return result;
    }

    /* (non-Javadoc)
     * @see java.io.Externalizable#writeExternal(java.io.ObjectOutput)
     */
    public void writeExternal(ObjectOutput out) throws IOException{
        out.writeLong(previousItem);
        out.writeLong(nextItem);
        out.writeLong(referenceItem);
        
    }

    /* (non-Javadoc)
     * @see java.io.Externalizable#readExternal(java.io.ObjectInput)
     */
    public void readExternal(ObjectInput in) throws IOException,ClassNotFoundException{
       previousItem = in.readLong();
       nextItem = in.readLong();
       referenceItem = in.readLong();
        
    }
}