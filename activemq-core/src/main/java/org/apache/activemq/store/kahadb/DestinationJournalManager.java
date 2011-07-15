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
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.util.IOHelper;
import org.apache.kahadb.journal.DataFile;
import org.apache.kahadb.journal.Journal;

public class DestinationJournalManager implements JournalManager {
    private static final String PREPEND = "JournalDest-";
    private static final String QUEUE_PREPEND = PREPEND + "Queue-";
    private static final String TOPIC_PREPEND = PREPEND + "Topic-";
    private AtomicBoolean started = new AtomicBoolean();
    private final Map<ActiveMQDestination, Journal> journalMap = new ConcurrentHashMap<ActiveMQDestination, Journal>();
    private File directory = new File("KahaDB");
    private File directoryArchive;
    private int maxFileLength = Journal.DEFAULT_MAX_FILE_LENGTH;
    private boolean checkForCorruptionOnStartup;
    private boolean checksum = false;
    private int writeBatchSize = Journal.DEFAULT_MAX_WRITE_BATCH_SIZE;
    private boolean archiveDataLogs;
    private AtomicLong storeSize = new AtomicLong(0);


    public AtomicBoolean getStarted() {
        return started;
    }

    public void setStarted(AtomicBoolean started) {
        this.started = started;
    }

    public File getDirectory() {
        return directory;
    }

    public void setDirectory(File directory) {
        this.directory = directory;
    }

    public File getDirectoryArchive() {
        return directoryArchive;
    }

    public void setDirectoryArchive(File directoryArchive) {
        this.directoryArchive = directoryArchive;
    }

    public int getMaxFileLength() {
        return maxFileLength;
    }

    public void setMaxFileLength(int maxFileLength) {
        this.maxFileLength = maxFileLength;
    }

    public boolean isCheckForCorruptionOnStartup() {
        return checkForCorruptionOnStartup;
    }

    public void setCheckForCorruptionOnStartup(boolean checkForCorruptionOnStartup) {
        this.checkForCorruptionOnStartup = checkForCorruptionOnStartup;
    }

    public boolean isChecksum() {
        return checksum;
    }

    public void setChecksum(boolean checksum) {
        this.checksum = checksum;
    }

    public int getWriteBatchSize() {
        return writeBatchSize;
    }

    public void setWriteBatchSize(int writeBatchSize) {
        this.writeBatchSize = writeBatchSize;
    }

    public boolean isArchiveDataLogs() {
        return archiveDataLogs;
    }

    public void setArchiveDataLogs(boolean archiveDataLogs) {
        this.archiveDataLogs = archiveDataLogs;
    }

    public AtomicLong getStoreSize() {
        return storeSize;
    }

    public void setStoreSize(AtomicLong storeSize) {
        this.storeSize = storeSize;
    }


    public void start() throws IOException {
        if (started.compareAndSet(false, true)) {
            File[] files = getDirectory().listFiles(new FilenameFilter() {
                public boolean accept(File file, String s) {
                    if (file.isDirectory() && s != null && s.startsWith(PREPEND)) {
                        return true;
                    }
                    return false;
                }
            });
            if (files != null) {
                for (File file : files) {
                    ActiveMQDestination destination;
                    if (file.getName().startsWith(TOPIC_PREPEND)) {
                        String destinationName = file.getName().substring(TOPIC_PREPEND.length());
                        destination = new ActiveMQTopic(destinationName);
                    } else {
                        String destinationName = file.getName().substring(QUEUE_PREPEND.length());
                        destination = new ActiveMQQueue(destinationName);
                    }

                    Journal journal = new Journal();
                    journal.setDirectory(file);
                    if (getDirectoryArchive() != null) {
                        IOHelper.mkdirs(getDirectoryArchive());
                        File archive = new File(getDirectoryArchive(), file.getName());
                        IOHelper.mkdirs(archive);
                        journal.setDirectoryArchive(archive);
                    }
                    configure(journal);
                    journalMap.put(destination, journal);
                }
            }
            for (Journal journal : journalMap.values()) {
                journal.start();
            }
        }

    }

    public void close() throws IOException {
        started.set(false);
        for (Journal journal : journalMap.values()) {
            journal.close();
        }
        journalMap.clear();
    }


    public void delete() throws IOException {
        for (Journal journal : journalMap.values()) {
            journal.delete();
        }
        journalMap.clear();
    }

    public Journal getJournal(ActiveMQDestination destination) throws IOException {
        Journal journal = journalMap.get(destination);
        if (journal == null && !destination.isTemporary()) {
            journal = new Journal();
            String fileName;
            if (destination.isTopic()) {
                fileName = TOPIC_PREPEND + destination.getPhysicalName();
            } else {
                fileName = QUEUE_PREPEND + destination.getPhysicalName();
            }
            File file = new File(getDirectory(), fileName);
            IOHelper.mkdirs(file);
            journal.setDirectory(file);
            if (getDirectoryArchive() != null) {
                IOHelper.mkdirs(getDirectoryArchive());
                File archive = new File(getDirectoryArchive(), fileName);
                IOHelper.mkdirs(archive);
                journal.setDirectoryArchive(archive);
            }
            configure(journal);
            if (started.get()) {
                journal.start();
            }
            return journal;
        } else {
            return journal;
        }
    }

    public Map<Integer, DataFile> getFileMap() {
        throw new RuntimeException("Not supported");
    }

    public Collection<Journal> getJournals() {
        return journalMap.values();
    }

    public Collection<Journal> getJournals(Set<ActiveMQDestination> set) {
        List<Journal> list = new ArrayList<Journal>();
        for (ActiveMQDestination destination : set) {
            Journal j = journalMap.get(destination);
            if (j != null) {
                list.add(j);
            }
        }
        return list;
    }

    protected void configure(Journal journal) {
        journal.setMaxFileLength(getMaxFileLength());
        journal.setCheckForCorruptionOnStartup(isCheckForCorruptionOnStartup());
        journal.setChecksum(isChecksum() || isCheckForCorruptionOnStartup());
        journal.setWriteBatchSize(getWriteBatchSize());
        journal.setArchiveDataLogs(isArchiveDataLogs());
        journal.setSizeAccumulator(getStoreSize());
    }
}
