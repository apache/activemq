/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import java.io.RandomAccessFile;
import java.util.LinkedList;

import org.apache.activemq.kaha.impl.data.DataManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
/**
 * Optimized Store reader
 * 
 * @version $Revision: 1.1.1.1 $
 */
public final class IndexManager{
    private static final Log log=LogFactory.getLog(IndexManager.class);
    private static final String NAME_PREFIX="index-";
    private final String name;
    private File file;
    private RandomAccessFile indexFile;
    private StoreIndexReader reader;
    private StoreIndexWriter writer;
    private LinkedList freeList=new LinkedList();
    private long length=0;

    public IndexManager(File directory,String name,String mode,DataManager redoLog) throws IOException{
        this.name=name;
        file=new File(directory,NAME_PREFIX+name);
        indexFile=new RandomAccessFile(file,mode);
        reader=new StoreIndexReader(indexFile);
        writer=new StoreIndexWriter(indexFile,name,redoLog);
        long offset=0;
        while((offset+IndexItem.INDEX_SIZE)<=indexFile.length()){
            IndexItem index=reader.readItem(offset);
            if(!index.isActive()){
                index.reset();
                freeList.add(index);
            }
            offset+=IndexItem.INDEX_SIZE;
        }
        length=offset;
    }

    public synchronized boolean isEmpty(){
        return freeList.isEmpty()&&length==0;
    }

    public synchronized IndexItem getIndex(long offset) throws IOException{
        return reader.readItem(offset);
    }

    public synchronized void freeIndex(IndexItem item) throws IOException{
        item.reset();
        item.setActive(false);
        writer.storeItem(item);
        freeList.add(item);
    }

    public synchronized void updateIndex(IndexItem index) throws IOException{
        writer.storeItem(index);
    }

    public void redo(final RedoStoreIndexItem redo) throws IOException{
        writer.redoStoreItem(redo);
    }

    public synchronized IndexItem createNewIndex(){
        IndexItem result=getNextFreeIndex();
        if(result==null){
            // allocate one
            result=new IndexItem();
            result.setOffset(length);
            length+=IndexItem.INDEX_SIZE;
        }
        return result;
    }

    public synchronized void close() throws IOException{
        if(indexFile!=null){
            indexFile.close();
            indexFile=null;
        }
    }

    public synchronized void force() throws IOException{
        if(indexFile!=null){
            indexFile.getFD().sync();
        }
    }

    public synchronized boolean delete() throws IOException{
        freeList.clear();
        if(indexFile!=null){
            indexFile.close();
            indexFile=null;
        }
        return file.delete();
    }

    private IndexItem getNextFreeIndex(){
        IndexItem result=null;
        if(!freeList.isEmpty()){
            result=(IndexItem) freeList.removeLast();
            result.reset();
        }
        return result;
    }

    long getLength(){
        return length;
    }

    public void setLength(long value){
        this.length=value;
    }

    
    public String toString(){
        return "IndexManager:("+NAME_PREFIX+name+")";
    }
}
