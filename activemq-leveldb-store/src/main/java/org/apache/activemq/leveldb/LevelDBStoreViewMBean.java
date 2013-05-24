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

import org.apache.activemq.broker.jmx.MBeanInfo;

import java.io.File;

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public interface LevelDBStoreViewMBean {

    @MBeanInfo("The directory holding the store index data.")
    String getIndexDirectory();

    @MBeanInfo("The directory holding the store log data.")
    String getLogDirectory();

    @MBeanInfo("The size the log files are allowed to grow to.")
    long getLogSize();

    @MBeanInfo("The implementation of the LevelDB index being used.")
    String getIndexFactory();

    @MBeanInfo("Are writes synced to disk.")
    boolean getSync();

    @MBeanInfo("Is data verified against checksums as it's loaded back from disk.")
    boolean getVerifyChecksums();

    @MBeanInfo("The maximum number of open files the index will open at one time.")
    int getIndexMaxOpenFiles();

    @MBeanInfo("Number of keys between restart points for delta encoding of keys in the index")
    int getIndexBlockRestartInterval();

    @MBeanInfo("Do aggressive checking of store data")
    boolean getParanoidChecks();

    @MBeanInfo("Amount of data to build up in memory for the index before converting to a sorted on-disk file.")
    int getIndexWriteBufferSize();

    @MBeanInfo("Approximate size of user data packed per block for the index")
    int getIndexBlockSize();

    @MBeanInfo("The type of compression to use for the index")
    String getIndexCompression();

    @MBeanInfo("The size of the cache index")
    long getIndexCacheSize();

    @MBeanInfo("The maximum amount of async writes to buffer up")
    int getAsyncBufferSize();

    @MBeanInfo("The number of units of work which have been closed.")
    long getUowClosedCounter();
    @MBeanInfo("The number of units of work which have been canceled.")
    long getUowCanceledCounter();
    @MBeanInfo("The number of units of work which started getting stored.")
    long getUowStoringCounter();
    @MBeanInfo("The number of units of work which completed getting stored")
    long getUowStoredCounter();

    @MBeanInfo("Gets and resets the maximum time (in ms) a unit of work took to complete.")
    double resetUowMaxCompleteLatency();
    @MBeanInfo("Gets and resets the maximum time (in ms) an index write batch took to execute.")
    double resetMaxIndexWriteLatency();
    @MBeanInfo("Gets and resets the maximum time (in ms) a log write took to execute (includes the index write latency).")
    double resetMaxLogWriteLatency();
    @MBeanInfo("Gets and resets the maximum time (in ms) a log flush took to execute.")
    double resetMaxLogFlushLatency();
    @MBeanInfo("Gets and resets the maximum time (in ms) a log rotation took to perform.")
    double resetMaxLogRotateLatency();

    @MBeanInfo("Gets the maximum time (in ms) a unit of work took to complete.")
    double getUowMaxCompleteLatency();
    @MBeanInfo("Gets the maximum time (in ms) an index write batch took to execute.")
    double getMaxIndexWriteLatency();
    @MBeanInfo("Gets the maximum time (in ms) a log write took to execute (includes the index write latency).")
    double getMaxLogWriteLatency();
    @MBeanInfo("Gets the maximum time (in ms) a log flush took to execute.")
    double getMaxLogFlushLatency();
    @MBeanInfo("Gets the maximum time (in ms) a log rotation took to perform.")
    double getMaxLogRotateLatency();

    @MBeanInfo("Gets the index statistics.")
    String getIndexStats();

    @MBeanInfo("Compacts disk usage")
    void compact();

}
