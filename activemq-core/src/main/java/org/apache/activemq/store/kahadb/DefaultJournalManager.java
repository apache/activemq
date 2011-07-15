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
package org.apache.activemq.store.kahadb;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.kahadb.journal.DataFile;
import org.apache.kahadb.journal.Journal;


public class DefaultJournalManager implements JournalManager {

    private final Journal journal;
    private final List<Journal> journals;

    public DefaultJournalManager() {
        this.journal = new Journal();
        List<Journal> list = new ArrayList<Journal>(1);
        list.add(this.journal);
        this.journals = Collections.unmodifiableList(list);
    }

    public void start() throws IOException {
        journal.start();
    }

    public void close() throws IOException {
        journal.close();
    }

    public Journal getJournal(ActiveMQDestination destination) {
        return journal;
    }

    public void setDirectory(File directory) {
        journal.setDirectory(directory);
    }

    public void setMaxFileLength(int maxFileLength) {
        journal.setMaxFileLength(maxFileLength);
    }

    public void setCheckForCorruptionOnStartup(boolean checkForCorruptJournalFiles) {
        journal.setCheckForCorruptionOnStartup(checkForCorruptJournalFiles);
    }

    public void setChecksum(boolean checksum) {
        journal.setChecksum(checksum);
    }

    public void setWriteBatchSize(int batchSize) {
        journal.setWriteBatchSize(batchSize);
    }

    public void setArchiveDataLogs(boolean archiveDataLogs) {
        journal.setArchiveDataLogs(archiveDataLogs);
    }

    public void setStoreSize(AtomicLong storeSize) {
        journal.setSizeAccumulator(storeSize);
    }

    public void setDirectoryArchive(File directoryArchive) {
        journal.setDirectoryArchive(directoryArchive);
    }

    public void delete() throws IOException {
        journal.delete();
    }

    public Map<Integer, DataFile> getFileMap() {
        return journal.getFileMap();
    }

    public Collection<Journal> getJournals() {
        return journals;
    }

    public Collection<Journal> getJournals(Set<ActiveMQDestination> set) {
        return journals;
    }
}
