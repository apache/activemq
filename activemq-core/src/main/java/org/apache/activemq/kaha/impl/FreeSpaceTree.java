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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
/**
 * A a wrapper for a TreeMap of free Items - sorted by size This enables us to re-use free Items on disk
 * 
 * @version $Revision: 1.2 $
 */
class FreeSpaceTree{
    private Map sizeMap=new HashMap();
    private TreeMap tree=new TreeMap();

    void addItem(Item item){
        Long sizeKey=new Long(item.getSize());
        Item old=(Item) tree.put(sizeKey,item);
        if(old!=null){
            // We'll preserve old items to reuse
            List list=(List) sizeMap.get(sizeKey);
            if(list==null){
                list=new ArrayList();
                sizeMap.put(sizeKey,list);
            }
            list.add(old);
        }
    }

    boolean removeItem(Item item){
        boolean result=false;
        Long sizeKey=new Long(item.getSize());
        Item retrieved=(Item) tree.get(sizeKey);
        if(retrieved==item){
            Object foo=tree.remove(sizeKey);
            if(foo!=retrieved){
                Thread.dumpStack();
                System.exit(0);
            }
            result=true;
            reconfigureTree(sizeKey);
        }else{
            List list=(List) sizeMap.get(sizeKey);
            if(list!=null){
                boolean foo=list.remove(item);
                if(list.isEmpty()){
                    sizeMap.remove(sizeKey);
                }
            }
        }
        return result;
    }

    Item getNextFreeSpace(Item item){
        Item result=null;
        if(!tree.isEmpty()){
            Long sizeKey=new Long(item.getSize());
            SortedMap map=tree.tailMap(sizeKey);
            if(map!=null&&!map.isEmpty()){
                Long resultKey=(Long) map.firstKey();
                result=(Item) map.get(resultKey);
                if(result!=null){
                    // remove from the tree
                    tree.remove(resultKey);
                    reconfigureTree(resultKey);
                }
            }
        }
        return result;
    }

    void reconfigureTree(Long sizeKey){
        List list=(List) sizeMap.get(sizeKey);
        if(list!=null){
            if(!list.isEmpty()){
                Object newItem=list.remove(list.size()-1);
                tree.put(sizeKey,newItem);
            }
            if(list.isEmpty()){
                sizeMap.remove(sizeKey);
            }
        }
    }

    int size(){
        int result=0;
        for(Iterator i=sizeMap.values().iterator();i.hasNext();){
            List list=(List) i.next();
            result+=list.size();
        }
        return result+tree.size();
    }
}