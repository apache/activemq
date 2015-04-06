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
import java.io.RandomAccessFile;

/**
 * Allows you to open a data file in read only mode.  Useful when working with 
 * archived data files.
 */
public class ReadOnlyDataFile extends DataFile {

    ReadOnlyDataFile(File file, int number) {
        super(file, number);
    }
    
    public RandomAccessFile openRandomAccessFile(boolean appender) throws IOException {
        return new RandomAccessFile(file, "r");
    }

    public void closeRandomAccessFile(RandomAccessFile file) throws IOException {
        file.close();
    }

    public synchronized boolean delete() throws IOException {
        throw new RuntimeException("Not valid on a read only file.");
    }
    
    public synchronized void move(File targetDirectory) throws IOException{
        throw new RuntimeException("Not valid on a read only file.");
    }

}
