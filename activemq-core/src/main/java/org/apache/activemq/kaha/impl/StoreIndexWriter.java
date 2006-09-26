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

import java.io.IOException;
import java.io.RandomAccessFile;
/**
 * Optimized Store writer
 * 
 * @version $Revision: 1.1.1.1 $
 */
class StoreIndexWriter{
    protected StoreByteArrayOutputStream dataOut;
    protected RandomAccessFile file;

    /**
     * Construct a Store index writer
     * 
     * @param file
     */
    StoreIndexWriter(RandomAccessFile file){
        this.file=file;
        this.dataOut=new StoreByteArrayOutputStream();
    }

    void storeItem(IndexItem index) throws IOException{
        dataOut.reset();
        index.write(dataOut);
        file.seek(index.getOffset());
        file.write(dataOut.getData(),0,IndexItem.INDEX_SIZE);
    }
}
