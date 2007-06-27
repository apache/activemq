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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
/**
 * DataFile
 * 
 * @version $Revision: 1.1.1.1 $
 */
class DataFile{
	
    private File file;
    private Integer number;
    private int referenceCount;
    private RandomAccessFile randomAcessFile;
    private Object writerData;
    long length=0;
    private boolean dirty;

    DataFile(File file,int number){
        this.file=file;
        this.number=Integer.valueOf(number);
        length=file.exists()?file.length():0;
    }

    Integer getNumber(){
        return number;
    }

    synchronized RandomAccessFile getRandomAccessFile() throws FileNotFoundException{
        if(randomAcessFile==null){
            randomAcessFile=new RandomAccessFile(file,"rw");
        }
        return randomAcessFile;
    }

    synchronized long getLength(){
        return length;
    }

    synchronized void incrementLength(int size){
        length+=size;
    }

    synchronized void purge() throws IOException{
        if(randomAcessFile!=null){
            randomAcessFile.close();
            randomAcessFile=null;
        }
    }

    synchronized boolean delete() throws IOException{
        purge();
        return file.delete();
    }

    synchronized void close() throws IOException{
        if(randomAcessFile!=null){
            randomAcessFile.close();
        }
    }

    synchronized int increment(){
        return ++referenceCount;
    }

    synchronized int decrement(){
        return --referenceCount;
    }

    synchronized boolean isUnused(){
        return referenceCount<=0;
    }
    
    public String toString(){
        String result = file.getName() + " number = " + number + " , length = " + length + " refCount = " + referenceCount;
        return result;
    }

    /**
     * @return Opaque data that a DataFileWriter may want to associate with the DataFile.
     */
	public synchronized Object getWriterData() {
		return writerData;
	}

	/**
	 * @param writerData - Opaque data that a DataFileWriter may want to associate with the DataFile.
	 */
	public synchronized void setWriterData(Object writerData) {
		this.writerData = writerData;
        dirty=true;
	}
    
    public synchronized boolean isDirty() {
        return dirty;
    }
    
    public synchronized void setDirty(boolean value) {
        this.dirty = value;
    }

}
