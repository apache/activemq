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
import org.apache.activemq.kaha.Marshaller;
import org.apache.activemq.kaha.ObjectMarshaller;
import org.apache.activemq.kaha.RuntimeStoreException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
* A container of roots for other Containers
* 
* @version $Revision: 1.2 $
*/

class RootContainer extends MapContainerImpl{
    private static final Log log=LogFactory.getLog(RootContainer.class);
    protected static final Marshaller rootMarshaller = new ObjectMarshaller();
    
    protected RootContainer(Object id,StoreImpl rfs,LocatableItem root) throws IOException{
        super(id,rfs,root);
    }

    protected void addRoot(Object key,LocatableItem er) throws IOException{
        
            if(map.containsKey(key)){
                remove(key);
            }
            LocatableItem entry=writeRoot(key,er);
            map.put(key,entry);
            synchronized(list){
            list.add(entry);
            }
        
    }

    protected LocatableItem writeRoot(Object key,LocatableItem value){
        long pos=Item.POSITION_NOT_SET;
        LocatableItem item=null;
        try{
            if(value!=null){
                pos=store.storeItem(rootMarshaller,value,value);
            }
            LocatableItem last=list.isEmpty()?null:(LocatableItem) list.getLast();
            last=last==null?root:last;
            long prev=last.getOffset();
            long next=Item.POSITION_NOT_SET;
            item=new LocatableItem(prev,next,pos);
            if(log.isDebugEnabled())
                log.debug("writing root ...");
            if(log.isDebugEnabled())
                log.debug("root = "+value);
            next=store.storeItem(rootMarshaller,key,item);
            if(last!=null){
                last.setNextItem(next);
                store.updateItem(last);
            }
        }catch(IOException e){
            e.printStackTrace();
            log.error("Failed to write root",e);
            throw new RuntimeStoreException(e);
        }
        return item;
    }

    protected Object getValue(LocatableItem item){
        LocatableItem result=null;
        if(item!=null&&item.getReferenceItem()!=Item.POSITION_NOT_SET){
            LocatableItem value=new LocatableItem();
            value.setOffset(item.getReferenceItem());
            try{
                result=(LocatableItem) store.readItem(rootMarshaller,value);
                //now read the item
                result.setOffset(item.getReferenceItem());
                store.readItem(rootMarshaller, result);
            }catch(IOException e){
                log.error("Could not read item "+item,e);
                throw new RuntimeStoreException(e);
            }
        }
        return result;
    }

}