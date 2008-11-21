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
package org.apache.kahadb.journal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Used to pool DataFileAccessors.
 * 
 * @author chirino
 */
public class DataFileAccessorPool {

    private final Journal dataManager;
    private final Map<Integer, Pool> pools = new HashMap<Integer, Pool>();
    private boolean closed;
    private int maxOpenReadersPerFile = 5;

    class Pool {

        private final DataFile file;
        private final List<DataFileAccessor> pool = new ArrayList<DataFileAccessor>();
        private boolean used;
        private int openCounter;
        private boolean disposed;

        public Pool(DataFile file) {
            this.file = file;
        }

        public DataFileAccessor openDataFileReader() throws IOException {
            DataFileAccessor rc = null;
            if (pool.isEmpty()) {
                rc = new DataFileAccessor(dataManager, file);
            } else {
                rc = (DataFileAccessor)pool.remove(pool.size() - 1);
            }
            used = true;
            openCounter++;
            return rc;
        }

        public synchronized void closeDataFileReader(DataFileAccessor reader) {
            openCounter--;
            if (pool.size() >= maxOpenReadersPerFile || disposed) {
                reader.dispose();
            } else {
                pool.add(reader);
            }
        }

        public synchronized void clearUsedMark() {
            used = false;
        }

        public synchronized boolean isUsed() {
            return used;
        }

        public synchronized void dispose() {
            for (DataFileAccessor reader : pool) {
                reader.dispose();
            }
            pool.clear();
            disposed = true;
        }

        public synchronized int getOpenCounter() {
            return openCounter;
        }

    }

    public DataFileAccessorPool(Journal dataManager) {
        this.dataManager = dataManager;
    }

    synchronized void clearUsedMark() {
        for (Iterator iter = pools.values().iterator(); iter.hasNext();) {
            Pool pool = (Pool)iter.next();
            pool.clearUsedMark();
        }
    }

    synchronized void disposeUnused() {
        for (Iterator<Pool> iter = pools.values().iterator(); iter.hasNext();) {
            Pool pool = iter.next();
            if (!pool.isUsed()) {
                pool.dispose();
                iter.remove();
            }
        }
    }

    synchronized void disposeDataFileAccessors(DataFile dataFile) {
        if (closed) {
            throw new IllegalStateException("Closed.");
        }
        Pool pool = pools.get(dataFile.getDataFileId());
        if (pool != null) {
            if (pool.getOpenCounter() == 0) {
                pool.dispose();
                pools.remove(dataFile.getDataFileId());
            } else {
                throw new IllegalStateException("The data file is still in use: " + dataFile + ", use count: " + pool.getOpenCounter());
            }
        }
    }

    synchronized DataFileAccessor openDataFileAccessor(DataFile dataFile) throws IOException {
        if (closed) {
            throw new IOException("Closed.");
        }

        Pool pool = pools.get(dataFile.getDataFileId());
        if (pool == null) {
            pool = new Pool(dataFile);
            pools.put(dataFile.getDataFileId(), pool);
        }
        return pool.openDataFileReader();
    }

    synchronized void closeDataFileAccessor(DataFileAccessor reader) {
        Pool pool = pools.get(reader.getDataFile().getDataFileId());
        if (pool == null || closed) {
            reader.dispose();
        } else {
            pool.closeDataFileReader(reader);
        }
    }

    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        for (Iterator<Pool> iter = pools.values().iterator(); iter.hasNext();) {
            Pool pool = iter.next();
            pool.dispose();
        }
        pools.clear();
    }

}
