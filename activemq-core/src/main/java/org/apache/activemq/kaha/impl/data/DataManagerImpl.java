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
package org.apache.activemq.kaha.impl.data;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.activemq.kaha.Marshaller;
import org.apache.activemq.kaha.StoreLocation;
import org.apache.activemq.kaha.impl.DataManager;
import org.apache.activemq.kaha.impl.index.RedoStoreIndexItem;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
/**
 * Manages DataFiles
 * 
 * @version $Revision: 1.1.1.1 $
 */
public final class DataManagerImpl implements DataManager {
    
    private static final Log log=LogFactory.getLog(DataManagerImpl.class);
    public static final long MAX_FILE_LENGTH=1024*1024*32;
    private static final String NAME_PREFIX="data-";
    private final File directory;
    private final String name;
    private SyncDataFileReader reader;
    private SyncDataFileWriter writer;
    private DataFile currentWriteFile;
    private long maxFileLength = MAX_FILE_LENGTH;
    Map fileMap=new HashMap();
    
    public static final int ITEM_HEAD_SIZE=5; // type + length
    public static final byte DATA_ITEM_TYPE=1;
    public static final byte REDO_ITEM_TYPE=2;
    
    Marshaller redoMarshaller = RedoStoreIndexItem.MARSHALLER;
    private String dataFilePrefix;
   
    public DataManagerImpl(File dir, final String name){
        this.directory=dir;
        this.name=name;
        
        dataFilePrefix = NAME_PREFIX+name+"-";
        // build up list of current dataFiles
        File[] files=dir.listFiles(new FilenameFilter(){
            public boolean accept(File dir,String n){
                return dir.equals(directory)&&n.startsWith(dataFilePrefix);
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
        File file=new File(directory,fileName);
        DataFile result=new DataFile(file,num);
        fileMap.put(result.getNumber(),result);
        return result;
    }

    /* (non-Javadoc)
	 * @see org.apache.activemq.kaha.impl.data.IDataManager#getName()
	 */
    public String getName(){
        return name;
    }

    synchronized DataFile  findSpaceForData(DataItem item) throws IOException{
        if(currentWriteFile==null||((currentWriteFile.getLength()+item.getSize())>maxFileLength)){
            int nextNum=currentWriteFile!=null?currentWriteFile.getNumber().intValue()+1:1;
            if(currentWriteFile!=null&&currentWriteFile.isUnused()){
                removeDataFile(currentWriteFile);
            }
            currentWriteFile=createAndAddDataFile(nextNum);
        }
        item.setOffset(currentWriteFile.getLength());
        item.setFile(currentWriteFile.getNumber().intValue());        
        currentWriteFile.incrementLength(item.getSize()+ITEM_HEAD_SIZE);
        return currentWriteFile;
    }

    DataFile getDataFile(StoreLocation item) throws IOException{
        Integer key=Integer.valueOf(item.getFile());
        DataFile dataFile=(DataFile) fileMap.get(key);
        if(dataFile==null){
            log.error("Looking for key " + key + " but not found in fileMap: " + fileMap);
            throw new IOException("Could not locate data file "+NAME_PREFIX+name+"-"+item.getFile());
        }
        return dataFile;
    }
    
    /* (non-Javadoc)
	 * @see org.apache.activemq.kaha.impl.data.IDataManager#readItem(org.apache.activemq.kaha.Marshaller, org.apache.activemq.kaha.StoreLocation)
	 */
    public synchronized Object readItem(Marshaller marshaller, StoreLocation item) throws IOException{
        return getReader().readItem(marshaller,item);
    }

    /* (non-Javadoc)
	 * @see org.apache.activemq.kaha.impl.data.IDataManager#storeDataItem(org.apache.activemq.kaha.Marshaller, java.lang.Object)
	 */
    public synchronized StoreLocation storeDataItem(Marshaller marshaller, Object payload) throws IOException{
        return getWriter().storeItem(marshaller,payload, DATA_ITEM_TYPE);
    }
    
    /* (non-Javadoc)
	 * @see org.apache.activemq.kaha.impl.data.IDataManager#storeRedoItem(java.lang.Object)
	 */
    public synchronized StoreLocation storeRedoItem(Object payload) throws IOException{
        return getWriter().storeItem(redoMarshaller, payload, REDO_ITEM_TYPE);
    }
    
    /* (non-Javadoc)
	 * @see org.apache.activemq.kaha.impl.data.IDataManager#updateItem(org.apache.activemq.kaha.StoreLocation, org.apache.activemq.kaha.Marshaller, java.lang.Object)
	 */
    public synchronized void updateItem(StoreLocation location,Marshaller marshaller, Object payload) throws IOException {
        getWriter().updateItem((DataItem)location,marshaller,payload,DATA_ITEM_TYPE);
    }

    /* (non-Javadoc)
	 * @see org.apache.activemq.kaha.impl.data.IDataManager#recoverRedoItems(org.apache.activemq.kaha.impl.data.RedoListener)
	 */
    public synchronized void recoverRedoItems(RedoListener listener) throws IOException{
        
        // Nothing to recover if there is no current file.
        if( currentWriteFile == null )
            return;
        
        DataItem item = new DataItem();
        item.setFile(currentWriteFile.getNumber().intValue());
        item.setOffset(0);
        while( true ) {
            byte type;
            try {
                type = getReader().readDataItemSize(item);
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
    
    /* (non-Javadoc)
	 * @see org.apache.activemq.kaha.impl.data.IDataManager#close()
	 */
    public synchronized void close() throws IOException{
    	getWriter().close();
        for(Iterator i=fileMap.values().iterator();i.hasNext();){
            DataFile dataFile=(DataFile) i.next();
            getWriter().force(dataFile);
            dataFile.close();
        }
        fileMap.clear();
    }

    /* (non-Javadoc)
	 * @see org.apache.activemq.kaha.impl.data.IDataManager#force()
	 */
    public synchronized void force() throws IOException{
        for(Iterator i=fileMap.values().iterator();i.hasNext();){
            DataFile dataFile=(DataFile) i.next();
            getWriter().force(dataFile);
        }
    }

        
    /* (non-Javadoc)
	 * @see org.apache.activemq.kaha.impl.data.IDataManager#delete()
	 */
    public synchronized boolean delete() throws IOException{
        boolean result=true;
        for(Iterator i=fileMap.values().iterator();i.hasNext();){
            DataFile dataFile=(DataFile) i.next();
            result&=dataFile.delete();
        }
        fileMap.clear();
        return result;
    }
    

    /* (non-Javadoc)
	 * @see org.apache.activemq.kaha.impl.data.IDataManager#addInterestInFile(int)
	 */
    public synchronized void addInterestInFile(int file) throws IOException{
        if(file>=0){
            Integer key=Integer.valueOf(file);
            DataFile dataFile=(DataFile) fileMap.get(key);
            if(dataFile==null){
                dataFile=createAndAddDataFile(file);
            }
            addInterestInFile(dataFile);
        }
    }

    synchronized void addInterestInFile(DataFile dataFile){
        if(dataFile!=null){
            dataFile.increment();
        }
    }

    /* (non-Javadoc)
	 * @see org.apache.activemq.kaha.impl.data.IDataManager#removeInterestInFile(int)
	 */
    public synchronized void removeInterestInFile(int file) throws IOException{
        if(file>=0){
            Integer key=Integer.valueOf(file);
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

    /* (non-Javadoc)
	 * @see org.apache.activemq.kaha.impl.data.IDataManager#consolidateDataFiles()
	 */
    public synchronized void consolidateDataFiles() throws IOException{
        List purgeList=new ArrayList();
        for(Iterator i=fileMap.values().iterator();i.hasNext();){
            DataFile dataFile=(DataFile) i.next();
            if(dataFile.isUnused() && dataFile != currentWriteFile){
                purgeList.add(dataFile);
            }
        }
        for(int i=0;i<purgeList.size();i++){
            DataFile dataFile=(DataFile) purgeList.get(i);
            removeDataFile(dataFile);
        }
    }

    private void removeDataFile(DataFile dataFile) throws IOException{
        fileMap.remove(dataFile.getNumber());
        if(writer!=null){
            writer.force(dataFile);
        }
        boolean result=dataFile.delete();
        log.debug("discarding data file "+dataFile+(result?"successful ":"failed"));
    }

    /* (non-Javadoc)
	 * @see org.apache.activemq.kaha.impl.data.IDataManager#getRedoMarshaller()
	 */
    public Marshaller getRedoMarshaller() {
        return redoMarshaller;
    }

    /* (non-Javadoc)
	 * @see org.apache.activemq.kaha.impl.data.IDataManager#setRedoMarshaller(org.apache.activemq.kaha.Marshaller)
	 */
    public void setRedoMarshaller(Marshaller redoMarshaller) {
        this.redoMarshaller = redoMarshaller;
    }

    /**
     * @return the maxFileLength
     */
    public long getMaxFileLength(){
        return maxFileLength;
    }

    /**
     * @param maxFileLength the maxFileLength to set
     */
    public void setMaxFileLength(long maxFileLength){
        this.maxFileLength=maxFileLength;
    }
    
    public String toString(){
        return "DataManager:("+NAME_PREFIX+name+")";
    }

	public synchronized SyncDataFileReader getReader() {
		if( reader == null ) {
			reader = createReader();
		}
		return reader;
	}
	protected synchronized SyncDataFileReader createReader() {
		return new SyncDataFileReader(this);
	}
	public synchronized void setReader(SyncDataFileReader reader) {
		this.reader = reader;
	}

	public synchronized SyncDataFileWriter getWriter() {
		if( writer==null ) {
			writer = createWriter();
		}
		return writer;
	}
	private SyncDataFileWriter createWriter() {
		return new SyncDataFileWriter(this);
	}
	public synchronized void setWriter(SyncDataFileWriter writer) {
		this.writer = writer;
	}

}
