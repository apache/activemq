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
package org.apache.activemq.spring.geronimo;

import java.io.File;

import org.apache.geronimo.transaction.log.HOWLLog;
import org.apache.geronimo.transaction.manager.TransactionLog;
import org.apache.geronimo.transaction.manager.XidFactory;
import org.apache.geronimo.transaction.manager.XidFactoryImpl;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;

/**
 * @version $Revision$ $Date$
 */
public class HowlLogFactoryBean implements FactoryBean, DisposableBean {
    private HOWLLog howlLog;

    private String logFileDir;
    private XidFactory xidFactory;

    private String bufferClassName = "org.objectweb.howl.log.BlockLogBuffer";
    private int bufferSizeKBytes = 32;
    private boolean checksumEnabled = true;
    private boolean adler32Checksum = true;
    private int flushSleepTimeMilliseconds = 50;
    private String logFileExt = "log";
    private String logFileName = "transaction";
    private int maxBlocksPerFile = -1;
    private int maxLogFiles = 2;
    private int maxBuffers = 0;
    private int minBuffers = 4;
    private int threadsWaitingForceThreshold = -1;
    private File serverBaseDir;

    public HowlLogFactoryBean() {
        String serverDir = System.getProperty("jencks.server.dir", System.getProperty("basedir", System.getProperty("user.dir")));
        serverBaseDir = new File(serverDir);
    }

    public Object getObject() throws Exception {
        if (howlLog == null) {
            howlLog = new HOWLLog(bufferClassName,
                    bufferSizeKBytes,
                    checksumEnabled,
                    adler32Checksum,
                    flushSleepTimeMilliseconds,
                    logFileDir,
                    logFileExt,
                    logFileName,
                    maxBlocksPerFile,
                    maxBuffers,
                    maxLogFiles,
                    minBuffers,
                    threadsWaitingForceThreshold,
                    xidFactory != null ? xidFactory : createXidFactory(),
                    serverBaseDir);

            howlLog.doStart();
        }
        return howlLog;
    }

    public void destroy() throws Exception {
        if (howlLog != null) {
            howlLog.doStop();
            howlLog = null;
        }
    }

    public Class<?> getObjectType() {
        return TransactionLog.class;
    }

    public boolean isSingleton() {
        return true;
    }

    public String getBufferClassName() {
        return bufferClassName;
    }

    public void setBufferClassName(String bufferClassName) {
        this.bufferClassName = bufferClassName;
    }

    public int getBufferSizeKBytes() {
        return bufferSizeKBytes;
    }

    public void setBufferSizeKBytes(int bufferSizeKBytes) {
        this.bufferSizeKBytes = bufferSizeKBytes;
    }

    public boolean isChecksumEnabled() {
        return checksumEnabled;
    }

    public void setChecksumEnabled(boolean checksumEnabled) {
        this.checksumEnabled = checksumEnabled;
    }

    public boolean isAdler32Checksum() {
        return adler32Checksum;
    }

    public void setAdler32Checksum(boolean adler32Checksum) {
        this.adler32Checksum = adler32Checksum;
    }

    public int getFlushSleepTimeMilliseconds() {
        return flushSleepTimeMilliseconds;
    }

    public void setFlushSleepTimeMilliseconds(int flushSleepTimeMilliseconds) {
        this.flushSleepTimeMilliseconds = flushSleepTimeMilliseconds;
    }

    public String getLogFileDir() {
        return logFileDir;
    }

    public void setLogFileDir(String logFileDir) {
        this.logFileDir = logFileDir;
    }

    public String getLogFileExt() {
        return logFileExt;
    }

    public void setLogFileExt(String logFileExt) {
        this.logFileExt = logFileExt;
    }

    public String getLogFileName() {
        return logFileName;
    }

    public void setLogFileName(String logFileName) {
        this.logFileName = logFileName;
    }

    public int getMaxBlocksPerFile() {
        return maxBlocksPerFile;
    }

    public void setMaxBlocksPerFile(int maxBlocksPerFile) {
        this.maxBlocksPerFile = maxBlocksPerFile;
    }

    public int getMaxBuffers() {
        return maxBuffers;
    }

    public void setMaxBuffers(int maxBuffers) {
        this.maxBuffers = maxBuffers;
    }

    public int getMaxLogFiles() {
        return maxLogFiles;
    }

    public void setMaxLogFiles(int maxLogFiles) {
        this.maxLogFiles = maxLogFiles;
    }

    public int getMinBuffers() {
        return minBuffers;
    }

    public void setMinBuffers(int minBuffers) {
        this.minBuffers = minBuffers;
    }

    public int getThreadsWaitingForceThreshold() {
        return threadsWaitingForceThreshold;
    }

    public void setThreadsWaitingForceThreshold(int threadsWaitingForceThreshold) {
        this.threadsWaitingForceThreshold = threadsWaitingForceThreshold;
    }

    public XidFactory getXidFactory() {
        return xidFactory;
    }

    public void setXidFactory(XidFactory xidFactory) {
        this.xidFactory = xidFactory;
    }

    public File getServerBaseDir() {
        return serverBaseDir;
    }

    public void setServerBaseDir(File serverBaseDir) {
        this.serverBaseDir = serverBaseDir;
    }

    public static XidFactory createXidFactory() {
        XidFactory xidFactory;
        xidFactory = new XidFactoryImpl();
        return xidFactory;
    }
}
