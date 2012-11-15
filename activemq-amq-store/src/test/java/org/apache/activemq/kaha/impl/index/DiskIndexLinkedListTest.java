/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.kaha.impl.index;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.kaha.impl.DataManager;
import org.apache.activemq.kaha.impl.data.DataManagerImpl;
import org.apache.activemq.kaha.impl.data.Item;
import org.apache.activemq.util.IOHelper;


public class DiskIndexLinkedListTest extends VMIndexLinkedListTest {

    private IndexManager im;
    protected IndexLinkedList createList(IndexItem root) throws IOException {
        String dirName = System.getProperty("basedir", ".") + "/target/activemq-data/testIndex";
        File file = new File(dirName);
        file.mkdirs();
        IOHelper.deleteChildren(file);
        DataManager dm = new DataManagerImpl(file,"test",new AtomicLong());
        im = new IndexManager(file,"test","rw",dm,new AtomicLong());
        root = im.createNewIndex();
        im.storeIndex(root);
        return new DiskIndexLinkedList(im,root);
    }
    
    IndexItem createIndex(IndexLinkedList indexList,int offset) throws IOException {
        IndexItem result =  im.createNewIndex();
        im.storeIndex(result);
        return result;
    }
    
    protected void addToList(IndexLinkedList list,IndexItem item) throws IOException {
        IndexItem root = list.getRoot();
        IndexItem prev = list.getLast();
        prev = prev != null ? prev : root;
        IndexItem next = list.getNextEntry(prev);
        prev.setNextItem(item.getOffset());
        item.setPreviousItem(prev.getOffset());
        im.updateIndexes(prev);
        if (next != null) {
            next.setPreviousItem(item.getOffset());
            item.setNextItem(next.getOffset());
            im.updateIndexes(next);
        }
        im.storeIndex(item);
        list.add(item);
    }
    
    protected void insertToList(IndexLinkedList list,int pos,IndexItem item) throws IOException {
        IndexItem root = list.getRoot();
        IndexItem prev = null;
        IndexItem next = null;
        if (pos <= 0) {
            prev = root;
            next = list.getNextEntry(root);
        } else if (pos >= list.size()) {
            prev = list.getLast();
            if (prev==null) {
                prev=root;
            }
            next = null;
        } else {
            prev = list.get(pos);
            prev = prev != null ? prev : root;
            next = list.getNextEntry(prev);
        }
        prev.setNextItem(item.getOffset());
        item.setPreviousItem(prev.getOffset());
        im.updateIndexes(prev);
        if (next != null) {
            next.setPreviousItem(item.getOffset());
            item.setNextItem(next.getOffset());
            im.updateIndexes(next);
        }
        im.storeIndex(item);
        list.setRoot(root);
        list.add(pos,item);
    }
    
    protected void insertFirst(IndexLinkedList list,IndexItem item) throws IOException {
        IndexItem root = list.getRoot();
        IndexItem prev = root;
        IndexItem next = list.getNextEntry(prev);
        prev.setNextItem(item.getOffset());
        item.setPreviousItem(prev.getOffset());
        im.updateIndexes(prev);
        if (next != null) {
            next.setPreviousItem(item.getOffset());
            item.setNextItem(next.getOffset());
            im.updateIndexes(next);
        }
        im.storeIndex(item);
        list.addFirst(item);
    }
    
    protected synchronized void remove(IndexLinkedList list,IndexItem item) throws IOException {
        IndexItem root = list.getRoot();
        IndexItem prev = list.getPrevEntry(item);
        IndexItem next = list.getNextEntry(item);
        list.remove(item);

        prev = prev == null ? root : prev;
        next = (next == null || !next.equals(root)) ? next : null;
       
        if (next != null) {
            prev.setNextItem(next.getOffset());
            next.setPreviousItem(prev.getOffset());
            im.updateIndexes(next);
        } else {
            prev.setNextItem(Item.POSITION_NOT_SET);
        }
        im.updateIndexes(prev);
    }
}
