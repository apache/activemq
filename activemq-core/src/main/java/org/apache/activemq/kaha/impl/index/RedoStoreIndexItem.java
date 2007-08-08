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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.apache.activemq.kaha.Marshaller;

public class RedoStoreIndexItem implements Externalizable {

    public static final Marshaller MARSHALLER = new Marshaller() {
        public Object readPayload(DataInput in) throws IOException {
            RedoStoreIndexItem item = new RedoStoreIndexItem();
            item.readExternal(in);
            return item;
        }

        public void writePayload(Object object, DataOutput out) throws IOException {
            RedoStoreIndexItem item = (RedoStoreIndexItem) object;
            item.writeExternal(out);
        }
    }; 
    
    private static final long serialVersionUID = -4865508871719676655L;
    private String indexName;
    private IndexItem indexItem;
    private long offset;

    public RedoStoreIndexItem() {
    }
    public RedoStoreIndexItem(String indexName, long offset, IndexItem item) {
        this.indexName = indexName;
        this.offset=offset;
        this.indexItem = item;
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        readExternal((DataInput)in);
    }
    public void readExternal(DataInput in) throws IOException {
        // indexName = in.readUTF();
        offset = in.readLong();
        indexItem = new IndexItem();
        indexItem.read(in);
    }
    
    public void writeExternal(ObjectOutput out) throws IOException {
        writeExternal((DataOutput)out);
    }
    public void writeExternal(DataOutput out) throws IOException {
        // out.writeUTF(indexName);
        out.writeLong(offset);
        indexItem.write(out);
    }
    
    public String getIndexName() {
        return indexName;
    }
    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }
    
    public IndexItem getIndexItem() {
        return indexItem;
    }
    public void setIndexItem(IndexItem item) {
        this.indexItem = item;
    }
    public long getOffset() {
        return offset;
    }
    public void setOffset(long offset) {
        this.offset = offset;
    }

}
