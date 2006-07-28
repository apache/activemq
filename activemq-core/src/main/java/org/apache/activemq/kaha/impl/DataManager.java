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
package org.apache.activemq.kaha.impl;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.activemq.kaha.Marshaller;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
/**
 * Manages DataFiles
 * 
 * @version $Revision: 1.1.1.1 $
 */
final class DataManager{
    
    private static final Log log=LogFactory.getLog(DataManager.class);
    protected static long MAX_FILE_LENGTH=1024*1024*32;
    private final File dir;
    private final String name;
    private StoreDataReader reader;
    private StoreDataWriter writer;
    private DataFile currentWriteFile;
    Map fileMap=new HashMap();

    public static final int ITEM_HEAD_SIZE=5; // type + length
    public static final byte DATA_ITEM_TYPE=1;
    public static final byte REDO_ITEM_TYPE=2;
    
    Marshaller redoMarshaller = RedoStoreIndexItem.MARSHALLER;
    private String dataFilePrefix;

    DataManager(File dir, final String name){
        this.dir=dir;
        this.name=name;
        this.reader=new StoreDataReader(this);
        this.writer=new StoreDataWriter(this);
        
        dataFilePrefix = "data-"+name+"-";
        // build up list of current dataFiles
        File[] files=dir.listFiles(new FilenameFilter(){
            public boolean accept(File dir,String n){
                return dir.equals(dir)&&n.startsWith(dataFilePrefix);
            }
        });
        if(files!=null){
            for(int i=0;i<files.length;i++){
                File file=files[i];
                String n=file.getName();
                String numStr=n.substring(dataFilePrefix.length(),n.length());
                int num=Integer.parseInt(numStr);
                DataFile dataFile=new DataFile(file,num);
                fileMap.put(dataFile.getNumber(),dataFile);
                if(currentWriteFile==null||currentWriteFile.getNumber().intValue()<num){
                    currentWriteFile=dataFile;
                }
            }
        }
    }
    
    private DataFile createAndAddDataFile(int num){
        String fileName=dataFilePrefix+num;
        File file=new File(dir,fileName);
        DataFile result=new DataFile(file,num);
        fileMap.put(result.getNumber(),result);
        return result;
    }

    public String getName(){
        return name;
    }

    DataFile findSpaceForData(DataItem item) throws IOException{
        if(currentWriteFile==null||((currentWriteFile.getLength()+item.getSize())>MAX_FILE_LENGTH)){
            int nextNum=currentWriteFile!=null?currentWriteFile.getNumber().intValue()+1:1;
            if(currentWriteFile!=null&&currentWriteFile.isUnused()){
                removeDataFile(currentWriteFile);
            }
            currentWriteFile=createAndAddDataFile(nextNum);
        }
        item.setOffset(currentWriteFile.getLength());
        item.setFile(currentWriteFile.getNumber().intValue());
        return currentWriteFile;
    }

    RandomAccessFile getDataFile(DataItem item) throws IOException{
        Integer key=new Integer(item.getFile());
        DataFile dataFile=(DataFile) fileMap.get(key);
        if(dataFile!=null){
            return dataFile.getRandomAccessFile();
        }
        throw new IOException("Could not locate data file "+name+item.getFile());
    }
    
    synchronized Object readItem(Marshaller marshaller, DataItem item) throws IOException{
        return reader.readItem(marshaller,item);
    }

    synchronized DataItem storeDataItem(Marshaller marshaller, Object payload) throws IOException{
        return writer.storeItem(marshaller,payload, DATA_ITEM_TYPE);
    }
    
    synchronized DataItem storeRedoItem(Object payload) throws IOException{
        return writer.storeItem(redoMarshaller, payload, REDO_ITEM_TYPE);
    }

    synchronized void recoverRedoItems(RedoListener listener) throws IOException{
        
        // Nothing to recover if there is no current file.
        if( currentWriteFile == null )
            return;
        
        DataItem item = new DataItem();
        item.setFile(currentWriteFile.getNumber().intValue());
        item.setOffset(0);
        while( true ) {
            byte type;
            try {
                type = reader.readDataItemSize(item);
            } catch (IOException ignore) {
                log.trace("End of data file reached at (header was invalid): "+item);
                return;
            }
            if( type == REDO_ITEM_TYPE ) {
                // Un-marshal the redo item
                Object object;
                try {
                    object = readItem(redoMarshaller, item);
                } catch (IOException e1) {
                    log.trace("End of data file reached at (payload was invalid): "+item);
                    return;
                }
                try {
                    
                    listener.onRedoItem(item, object);
                    // in case the listener is holding on to item references, copy it
                    // so we don't change it behind the listener's back.
                    item = item.copy();
                    
                } catch (Exception e) {
                    throw IOExceptionSupport.create("Recovery handler failed: "+e,e);
                }
            }
            // Move to the next item.
            item.setOffset(item.getOffset()+ITEM_HEAD_SIZE+item.getSize());
        }
    }
    
    synchronized void close() throws IOException{
        for(Iterator i=fileMap.values().iterator();i.hasNext();){
            DataFile dataFile=(DataFile) i.next();
            dataFile.force();
            dataFile.close();
        }
        fileMap.clear();
    }

    synchronized void force() throws IOException{
        for(Iterator i=fileMap.values().iterator();i.hasNext();){
            DataFile dataFile=(DataFile) i.next();
            dataFile.force();
        }
    }

    synchronized boolean delete() throws IOException{
        boolean result=true;
        for(Iterator i=fileMap.values().iterator();i.hasNext();){
            DataFile dataFile=(DataFile) i.next();
            result&=dataFile.delete();
        }
        fileMap.clear();
        return result;
    }

    synchronized void addInterestInFile(int file) throws IOException{
        if(file>=0){
            Integer key=new Integer(file);
            DataFile dataFile=(DataFile) fileMap.get(key);
            if(dataFile==null){
                dataFile=createAndAddDataFile(file);
            }
            addInterestInFile(dataFile);
        }
    }

    void addInterestInFile(DataFile dataFile){
        if(dataFile!=null){
            dataFile.increment();
        }
    }

    synchronized void removeInterestInFile(int file) throws IOException{
        if(file>=0){
            Integer key=new Integer(file);
            DataFile dataFile=(DataFile) fileMap.get(key);
            removeInterestInFile(dataFile);
        }
    }

    synchronized void removeInterestInFile(DataFile dataFile) throws IOException{
        if(dataFile!=null){
            if(dataFile.decrement()<=0){
                if(dataFile!=currentWriteFile){
                    removeDataFile(dataFile);
                }
            }
        }
    }

    synchronized void consolidateDataFiles() throws IOException{
        List purgeList=new ArrayList();
        for(Iterator i=fileMap.values().iterator();i.hasNext();){
            DataFile dataFile=(DataFile) i.next();
            if(dataFile.isUnused() && dataFile != currentWriteFile){
                purgeList.add(dataFile);
            }
        }
        for(int i=0;i<purgeList.size();i++){
            DataFile dataFile=(DataFile) purgeList.get(i);
            fileMap.remove(dataFile.getNumber());
            boolean result=dataFile.delete();
            log.debug("discarding data file "+dataFile+(result?"successful ":"failed"));
        }
    }

    private void removeDataFile(DataFile dataFile) throws IOException{
        fileMap.remove(dataFile.getNumber());
        boolean result=dataFile.delete();
        log.debug("discarding data file "+dataFile+(result?"successful ":"failed"));
    }

    public Marshaller getRedoMarshaller() {
        return redoMarshaller;
    }

    public void setRedoMarshaller(Marshaller redoMarshaller) {
        this.redoMarshaller = redoMarshaller;
    }
}
