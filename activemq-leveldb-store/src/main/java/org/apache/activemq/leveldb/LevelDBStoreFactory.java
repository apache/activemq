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
package org.apache.activemq.leveldb;

import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.PersistenceAdapterFactory;

import java.io.File;
import java.io.IOException;

/**
 * A factory which can create configured LevelDBStore objects.
 */
public class LevelDBStoreFactory implements PersistenceAdapterFactory {

    private int asyncBufferSize = 1024*1024*4;
    private File directory = new File("LevelDB");
    private int flushDelay = 1000*5;
    private int indexBlockRestartInterval = 16;
    private int indexBlockSize = 4 * 1024;
    private long indexCacheSize = 1024 * 1024 * 256L;
    private String indexCompression = "snappy";
    private String indexFactory = "org.fusesource.leveldbjni.JniDBFactory, org.iq80.leveldb.impl.Iq80DBFactory";
    private int indexMaxOpenFiles = 1000;
    private int indexWriteBufferSize = 1024*1024*6;
    private String logCompression = "none";
    private File logDirectory;
    private long logSize = 1024 * 1024 * 100;
    private boolean monitorStats;
    private boolean paranoidChecks;
    private boolean sync = true;
    private boolean verifyChecksums;


    @Override
    public PersistenceAdapter createPersistenceAdapter() throws IOException {
        LevelDBStore store = new LevelDBStore();
        store.setVerifyChecksums(verifyChecksums);
        store.setAsyncBufferSize(asyncBufferSize);
        store.setDirectory(directory);
        store.setFlushDelay(flushDelay);
        store.setIndexBlockRestartInterval(indexBlockRestartInterval);
        store.setIndexBlockSize(indexBlockSize);
        store.setIndexCacheSize(indexCacheSize);
        store.setIndexCompression(indexCompression);
        store.setIndexFactory(indexFactory);
        store.setIndexMaxOpenFiles(indexMaxOpenFiles);
        store.setIndexWriteBufferSize(indexWriteBufferSize);
        store.setLogCompression(logCompression);
        store.setLogDirectory(logDirectory);
        store.setLogSize(logSize);
        store.setMonitorStats(monitorStats);
        store.setParanoidChecks(paranoidChecks);
        store.setSync(sync);
        return store;
    }

    public int getAsyncBufferSize() {
        return asyncBufferSize;
    }

    public void setAsyncBufferSize(int asyncBufferSize) {
        this.asyncBufferSize = asyncBufferSize;
    }

    public File getDirectory() {
        return directory;
    }

    public void setDirectory(File directory) {
        this.directory = directory;
    }

    public int getFlushDelay() {
        return flushDelay;
    }

    public void setFlushDelay(int flushDelay) {
        this.flushDelay = flushDelay;
    }

    public int getIndexBlockRestartInterval() {
        return indexBlockRestartInterval;
    }

    public void setIndexBlockRestartInterval(int indexBlockRestartInterval) {
        this.indexBlockRestartInterval = indexBlockRestartInterval;
    }

    public int getIndexBlockSize() {
        return indexBlockSize;
    }

    public void setIndexBlockSize(int indexBlockSize) {
        this.indexBlockSize = indexBlockSize;
    }

    public long getIndexCacheSize() {
        return indexCacheSize;
    }

    public void setIndexCacheSize(long indexCacheSize) {
        this.indexCacheSize = indexCacheSize;
    }

    public String getIndexCompression() {
        return indexCompression;
    }

    public void setIndexCompression(String indexCompression) {
        this.indexCompression = indexCompression;
    }

    public String getIndexFactory() {
        return indexFactory;
    }

    public void setIndexFactory(String indexFactory) {
        this.indexFactory = indexFactory;
    }

    public int getIndexMaxOpenFiles() {
        return indexMaxOpenFiles;
    }

    public void setIndexMaxOpenFiles(int indexMaxOpenFiles) {
        this.indexMaxOpenFiles = indexMaxOpenFiles;
    }

    public int getIndexWriteBufferSize() {
        return indexWriteBufferSize;
    }

    public void setIndexWriteBufferSize(int indexWriteBufferSize) {
        this.indexWriteBufferSize = indexWriteBufferSize;
    }

    public String getLogCompression() {
        return logCompression;
    }

    public void setLogCompression(String logCompression) {
        this.logCompression = logCompression;
    }

    public File getLogDirectory() {
        return logDirectory;
    }

    public void setLogDirectory(File logDirectory) {
        this.logDirectory = logDirectory;
    }

    public long getLogSize() {
        return logSize;
    }

    public void setLogSize(long logSize) {
        this.logSize = logSize;
    }

    public boolean isMonitorStats() {
        return monitorStats;
    }

    public void setMonitorStats(boolean monitorStats) {
        this.monitorStats = monitorStats;
    }

    public boolean isParanoidChecks() {
        return paranoidChecks;
    }

    public void setParanoidChecks(boolean paranoidChecks) {
        this.paranoidChecks = paranoidChecks;
    }

    public boolean isSync() {
        return sync;
    }

    public void setSync(boolean sync) {
        this.sync = sync;
    }

    public boolean isVerifyChecksums() {
        return verifyChecksums;
    }

    public void setVerifyChecksums(boolean verifyChecksums) {
        this.verifyChecksums = verifyChecksums;
    }

}
