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
    private final String prefix;
    private StoreDataReader reader;
    private StoreDataWriter writer;
    private DataFile currentWriteFile;
    Map fileMap=new HashMap();

    DataManager(File dir,String pf){
        this.dir=dir;
        this.prefix=pf;
        this.reader=new StoreDataReader(this);
        this.writer=new StoreDataWriter(this);
        // build up list of current dataFiles
        File[] files=dir.listFiles(new FilenameFilter(){
            public boolean accept(File dir,String name){
                return dir.equals(dir)&&name.startsWith(prefix);
            }
        });
        if(files!=null){
            for(int i=0;i<files.length;i++){
                File file=files[i];
                String name=file.getName();
                String numStr=name.substring(prefix.length(),name.length());
                int num=Integer.parseInt(numStr);
                DataFile dataFile=new DataFile(file,num);
                fileMap.put(dataFile.getNumber(),dataFile);
                if(currentWriteFile==null||currentWriteFile.getNumber().intValue()<num){
                    currentWriteFile=dataFile;
                }
            }
        }
    }
    
    public String getPrefix(){
        return prefix;
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
        throw new IOException("Could not locate data file "+prefix+item.getFile());
    }

    synchronized Object readItem(Marshaller marshaller,DataItem item) throws IOException{
        return reader.readItem(marshaller,item);
    }

    synchronized DataItem storeItem(Marshaller marshaller,Object payload) throws IOException{
        return writer.storeItem(marshaller,payload);
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

    private DataFile createAndAddDataFile(int num){
        String fileName=prefix+num;
        File file=new File(dir,fileName);
        DataFile result=new DataFile(file,num);
        fileMap.put(result.getNumber(),result);
        return result;
    }

    private void removeDataFile(DataFile dataFile) throws IOException{
        fileMap.remove(dataFile.getNumber());
        boolean result=dataFile.delete();
        log.debug("discarding data file "+dataFile+(result?"successful ":"failed"));
    }
}
