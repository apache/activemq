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
package org.apache.activemq.store.amq.reader;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import javax.jms.InvalidSelectorException;
import javax.jms.Message;

import org.apache.activemq.command.DataStructure;
import org.apache.activemq.filter.BooleanExpression;
import org.apache.activemq.kaha.impl.async.AsyncDataManager;
import org.apache.activemq.kaha.impl.async.Location;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.selector.SelectorParser;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.wireformat.WireFormat;

/**
 * Reads and iterates through data log files for the AMQMessage Store
 * 
 */
public class AMQReader implements Iterable<Message> {

    private AsyncDataManager dataManager;
    private WireFormat wireFormat = new OpenWireFormat();
    private File file;
    private BooleanExpression expression;

    /**
     * List all the data files in a directory
     * @param directory
     * @return
     * @throws IOException
     */
    public static Set<File> listDataFiles(File directory) throws IOException{
        Set<File>result = new HashSet<File>();
        if (directory == null || !directory.exists() || !directory.isDirectory()) {
            throw new IOException("Invalid Directory " + directory);
        }
        AsyncDataManager dataManager = new AsyncDataManager();
        dataManager.setDirectory(directory);
        dataManager.start();
        Set<File> set = dataManager.getFiles();
        if (set != null) {
            result.addAll(set);
        }
        dataManager.close();
        return result;
    }
    /**
     * Create the AMQReader to read a directory of amq data logs - or an
     * individual data log file
     * 
     * @param file the directory - or file
     * @throws IOException 
     * @throws InvalidSelectorException 
     * @throws IOException
     * @throws InvalidSelectorException 
     */
    public AMQReader(File file) throws InvalidSelectorException, IOException {
        this(file,null);
    }
    
    /**
     * Create the AMQReader to read a directory of amq data logs - or an
     * individual data log file
     * 
     * @param file the directory - or file
     * @param selector the JMS selector or null to select all
     * @throws IOException
     * @throws InvalidSelectorException 
     */
    public AMQReader(File file, String selector) throws IOException, InvalidSelectorException {
        String str = selector != null ? selector.trim() : null;
        if (str != null && str.length() > 0) {
            this.expression=SelectorParser.parse(str);
        }
        dataManager = new AsyncDataManager();
        dataManager.setArchiveDataLogs(false);
        if (file.isDirectory()) {
            dataManager.setDirectory(file);
        } else {
            dataManager.setDirectory(file.getParentFile());
            dataManager.setDirectoryArchive(file);
            this.file = file;
        }
        dataManager.start();
    }

    public Iterator<Message> iterator() {
        return new AMQIterator(this,this.expression);
    }

    
    protected MessageLocation getNextMessage(MessageLocation lastLocation)
            throws IllegalStateException, IOException {
        if (this.file != null) {
            return getInternalNextMessage(this.file, lastLocation);
        }
        return getInternalNextMessage(lastLocation);
    }

    private MessageLocation getInternalNextMessage(MessageLocation lastLocation)
            throws IllegalStateException, IOException {
        return getInternalNextMessage(null, lastLocation);
    }

    private MessageLocation getInternalNextMessage(File file,
            MessageLocation lastLocation) throws IllegalStateException,
            IOException {
        MessageLocation result = lastLocation;
        if (result != null) {
            result.setMessage(null);
        }
        Message message = null;
        Location pos = lastLocation != null ? lastLocation.getLocation() : null;
        while ((pos = getNextLocation(file, pos)) != null) {
            message = getMessage(pos);
            if (message != null) {
                if (result == null) {
                    result = new MessageLocation();
                }
                result.setMessage(message);
                break;
            }
        }
        result.setLocation(pos);
        if (pos == null && message == null) {
            result = null;
        } else {
            result.setLocation(pos);
        }
        return result;
    }

    private Location getNextLocation(File file, Location last)
            throws IllegalStateException, IOException {
        if (file != null) {
            return dataManager.getNextLocation(file, last, true);
        }
        return dataManager.getNextLocation(last);
    }

    private Message getMessage(Location location) throws IOException {
        ByteSequence data = dataManager.read(location);
        DataStructure c = (DataStructure) wireFormat.unmarshal(data);
        if (c instanceof Message) {
            return (Message) c;
        }
        return null;

    }
}
