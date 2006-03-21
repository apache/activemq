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
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
/**
 * Free space list in the Store
 * 
 * @version $Revision: 1.2 $
 */
final class FreeSpaceManager{
    private static final Log log = LogFactory.getLog(FreeSpaceManager.class);
    static final int ROOT_SIZE=64;
    static final int RESIZE_INCREMENT=4096*1024;
    private Map map=new HashMap();
    private Map prevMap=new HashMap();
    private FreeSpaceTree tree=new FreeSpaceTree();
    private StoreWriter writer;
    private StoreReader reader;
    private long dataEnd=ROOT_SIZE;
    private long fileLength=-1;

    FreeSpaceManager(StoreWriter writer,StoreReader reader) throws IOException{
        this.writer=writer;
        this.reader=reader;
        this.fileLength=reader.length();
    }

    final Item getFreeSpace(Item item) throws IOException{
        Item result=tree.getNextFreeSpace(item);
        if(result==null){
            while(dataEnd>=fileLength){
                writer.allocateSpace(fileLength+RESIZE_INCREMENT);
                fileLength=reader.length();
            }
            result=new Item();
            result.setOffset(dataEnd);
            int newSize = ((item.getSize()/8)+1)*8;

            result.setSize(newSize);
            dataEnd=dataEnd+result.getSize()+Item.HEAD_SIZE;
        }else{
            removeFreeSpace(result);
        }
        // reset the item
        item.setActive(true);
        item.setOffset(result.getOffset());
        item.setSize(result.getSize());
        return item;
    }

    final void addFreeSpace(Item item) throws IOException{
        long currentOffset=reader.position();
        reader.readHeader(item);
        item.setActive(false);
        // see if we can condense some space together
        // first look for free space adjacent up the disk
        Long nextKey=new Long(item.getOffset()+item.getSize()+Item.HEAD_SIZE);
        Item next=(Item) map.remove(nextKey);
        if(next!=null){
            tree.removeItem(next);
            Long prevKey=new Long(next.getOffset()+next.getSize()+Item.HEAD_SIZE);
            prevMap.remove(prevKey);
            int newSize=item.getSize()+next.getSize()+Item.HEAD_SIZE;
            item.setSize(newSize);
        }
        // now see if there was a previous item
        // in the next map
        Long key=new Long(item.getOffset());
        Item prev=(Item) prevMap.remove(key);
        Long prevKey=prev!=null?new Long(prev.getOffset()):null;
        if(prev!=null&&prevKey!=null){
            // we can condense the free space
            // first we are about to change the item so remove it from the tree
            tree.removeItem(prev);
            int newSize=prev.getSize()+item.getSize()+Item.HEAD_SIZE;
            prev.setSize(newSize);
            // update the header
            writer.updateHeader(prev);
            // put back in the tree
            tree.addItem(prev);
        }else{
            // update the item header
            writer.updateHeader(item);
            tree.addItem(item);
            map.put(key,item);
            prevKey=new Long(item.getOffset()+item.getSize()+Item.HEAD_SIZE);
            prevMap.put(prevKey,item);
        }
        reader.position(currentOffset);
    }

    /**
     * validates and builds free list
     * 
     * @throws IOException
     */
    final void scanStoredItems() throws IOException{
        if(reader.length()>ROOT_SIZE){
            long offset=ROOT_SIZE;
            while((offset+Item.HEAD_SIZE)<reader.length()){
                Item item=new Item();
                try{
                    reader.position(offset);
                    item.setOffset(offset);
                    reader.readHeader(item);
                }catch(BadMagicException e){
                 
                    log.error("Got bad magic reading stored items",e);
                    break;
                }
                if(item.getSize()>=0){
                    if(!item.isActive()){
                        addFreeSpace(item);
                    }
                    offset+=item.getSize()+Item.HEAD_SIZE;
                }else{
                    // we've hit free space or end of file
                    break;
                }
            }
            dataEnd=offset;
        }else {
            dataEnd = ROOT_SIZE;
        }
    }

    private void removeFreeSpace(Item item){
        if(item!=null){
            long next=item.getOffset()+item.getSize()+Item.HEAD_SIZE;
            Long nextKey=new Long(next);
            prevMap.remove(nextKey);
            Long key=new Long(item.getOffset());
            map.remove(key);
        }
    }

    void dump(PrintWriter printer){
        printer.println("FreeSpace: map size = "+map.size()+", tree size = "+tree.size()+", prevMap size = "
                        +prevMap.size());
        for(Iterator i=map.entrySet().iterator();i.hasNext();){
            printer.println("map = "+i.next());
        }
    }
}
