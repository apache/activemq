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
package org.apache.kahadb.index;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.kahadb.page.Page;
import org.apache.kahadb.page.Transaction;
import org.apache.kahadb.util.VariableMarshaller;

/**
 * Bin in a HashIndex
 * 
 * @version $Revision$
 */
class HashBin<Key, Value> {
    
    
    static public class Marshaller<Key, Value> extends VariableMarshaller<HashBin<Key, Value>> {
        private final HashIndex<Key, Value> hashIndex;

        public Marshaller(HashIndex<Key, Value> index) {
            this.hashIndex = index;
        }
        
        public HashBin<Key, Value> readPayload(DataInput is) throws IOException {
            HashBin<Key, Value> bin = new HashBin<Key, Value>();
            int size = is.readInt();
            for(int i=0; i < size; i++) {
                Key key = hashIndex.getKeyMarshaller().readPayload(is);
                Value value = hashIndex.getValueMarshaller().readPayload(is);
                bin.data.put(key, value);
            }
            return bin;
        }

        public void writePayload(HashBin<Key, Value> bin, DataOutput os) throws IOException {
            os.writeInt(bin.data.size());
            for (Map.Entry<Key, Value> entry : bin.data.entrySet()) {
                hashIndex.getKeyMarshaller().writePayload(entry.getKey(), os);
                hashIndex.getValueMarshaller().writePayload(entry.getValue(), os);
            }
        }
        
    }
    
    private Page<HashBin<Key, Value>> page;
    private TreeMap<Key, Value> data = new TreeMap<Key, Value>();
    
    public int size() {
        return data.size();
    }

    public Value put(Key key, Value value) throws IOException {
        return data.put(key, value);
    }

    public Value get(Key key) throws IOException {
        return data.get(key);
    }
    
    public boolean containsKey(Key key) throws IOException {
        return data.containsKey(key);
    }
    
    public Map<Key, Value> getAll(Transaction tx) throws IOException {
        return data;
    }
    
    public Value remove(Key key) throws IOException {
        return data.remove(key);
    }

    public Page<HashBin<Key, Value>> getPage() {
        return page;
    }

    public void setPage(Page<HashBin<Key, Value>> page) {
        this.page = page;
        this.page.set(this);
    }


}
