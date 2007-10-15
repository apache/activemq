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
package org.apache.activemq.kaha.impl.async;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.apache.activemq.util.LinkedNode;

/**
 * DataFile
 * 
 * @version $Revision: 1.1.1.1 $
 */
class DataFile extends LinkedNode implements Comparable<DataFile> {

    private final File file;
    private final Integer dataFileId;
    private final int preferedSize;

    private int length;
    private int referenceCount;

    DataFile(File file, int number, int preferedSize) {
        this.file = file;
        this.preferedSize = preferedSize;
        this.dataFileId = Integer.valueOf(number);
        length = (int)(file.exists() ? file.length() : 0);
    }

    public Integer getDataFileId() {
        return dataFileId;
    }

    public synchronized int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public synchronized void incrementLength(int size) {
        length += size;
    }

    public synchronized int increment() {
        return ++referenceCount;
    }

    public synchronized int decrement() {
        return --referenceCount;
    }
    
    public synchronized int getReferenceCount(){
    	return referenceCount;
    }

    public synchronized boolean isUnused() {
        return referenceCount <= 0;
    }

    public synchronized String toString() {
        String result = file.getName() + " number = " + dataFileId + " , length = " + length + " refCount = " + referenceCount;
        return result;
    }

    public RandomAccessFile openRandomAccessFile(boolean appender) throws IOException {
        RandomAccessFile rc = new RandomAccessFile(file, "rw");
        // When we start to write files size them up so that the OS has a chance
        // to allocate the file contigously.
        if (appender) {
            if (length < preferedSize) {
                rc.setLength(preferedSize);
            }
        }
        return rc;
    }

    public void closeRandomAccessFile(RandomAccessFile file) throws IOException {
        // On close set the file size to the real size.
        if (length != file.length()) {
            file.setLength(getLength());
        }
        file.close();
    }

    public synchronized boolean delete() throws IOException {
        return file.delete();
    }

    public int compareTo(DataFile df) {
        return dataFileId - df.dataFileId;
    }

    @Override
    public boolean equals(Object o) {
        boolean result = false;
        if (o instanceof DataFile) {
            result = compareTo((DataFile)o) == 0;
        }
        return result;
    }

    @Override
    public int hashCode() {
        return dataFileId;
    }
}
