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
package org.apache.activemq.store.kahadb.disk.journal;

import java.io.File;
import java.io.IOException;

import org.apache.activemq.store.kahadb.disk.util.LinkedNode;
import org.apache.activemq.store.kahadb.disk.util.SequenceSet;
import org.apache.activemq.util.IOHelper;
import org.apache.activemq.util.RecoverableRandomAccessFile;

/**
 * DataFile
 */
public class DataFile extends LinkedNode<DataFile> implements Comparable<DataFile> {

    public final static byte STANDARD_LOG_FILE = 0x0;

    protected final File file;
    protected final Integer dataFileId;
    protected volatile int length;
    protected int typeCode = STANDARD_LOG_FILE;
    protected final SequenceSet corruptedBlocks = new SequenceSet();
    protected RecoverableRandomAccessFile appendRandomAccessFile;

    DataFile(File file, int number) {
        this.file = file;
        this.dataFileId = Integer.valueOf(number);
        length = (int)(file.exists() ? file.length() : 0);
    }

    public File getFile() {
        return file;
    }

    public Integer getDataFileId() {
        return dataFileId;
    }

    public int getTypeCode() {
        return typeCode;
    }

    public void setTypeCode(int typeCode) {
        this.typeCode = typeCode;
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

    public synchronized void decrementLength(int size) {
        length -= size;
    }

    @Override
    public synchronized String toString() {
        return file.getName() + " number = " + dataFileId + " , length = " + length;
    }

    public synchronized RecoverableRandomAccessFile appendRandomAccessFile() throws IOException {
        if (appendRandomAccessFile == null) {
            appendRandomAccessFile = new RecoverableRandomAccessFile(file.getCanonicalPath(), "rw");
        }
        return appendRandomAccessFile;
    }

    public synchronized RecoverableRandomAccessFile openRandomAccessFile() throws IOException {
        return new RecoverableRandomAccessFile(file.getCanonicalPath(), "rw");
    }

    public synchronized void closeRandomAccessFile(RecoverableRandomAccessFile file) throws IOException {
        file.close();
        if (file == appendRandomAccessFile) {
            appendRandomAccessFile = null;
        }
    }

    public synchronized boolean delete() throws IOException {
        return file.delete();
    }

    public synchronized void move(File targetDirectory) throws IOException{
        IOHelper.moveFile(file, targetDirectory);
    }

    public SequenceSet getCorruptedBlocks() {
        return corruptedBlocks;
    }

    @Override
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
