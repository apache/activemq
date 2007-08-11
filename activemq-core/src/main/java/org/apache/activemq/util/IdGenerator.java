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
package org.apache.activemq.util;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Generator for Globally unique Strings.
 */

public class IdGenerator {

    private static final Logger LOG = Logger.getLogger(IdGenerator.class.getName());
    private static final String UNIQUE_STUB;
    private static int instanceCount;
    private static String hostName;
    private String seed;
    private long sequence;

    static {
        String stub = "";
        boolean canAccessSystemProps = true;
        try {
            SecurityManager sm = System.getSecurityManager();
            if (sm != null) {
                sm.checkPropertiesAccess();
            }
        } catch (SecurityException se) {
            canAccessSystemProps = false;
        }

        if (canAccessSystemProps) {
            try {
                hostName = InetAddress.getLocalHost().getHostName();
                ServerSocket ss = new ServerSocket(0);
                stub = "-" + ss.getLocalPort() + "-" + System.currentTimeMillis() + "-";
                Thread.sleep(100);
                ss.close();
            } catch (Exception ioe) {
                LOG.log(Level.WARNING, "could not generate unique stub", ioe);
            }
        } else {
            hostName = "localhost";
            stub = "-1-" + System.currentTimeMillis() + "-";
        }
        UNIQUE_STUB = stub;
    }

    /**
     * Construct an IdGenerator
     */
    public IdGenerator(String prefix) {
        synchronized (UNIQUE_STUB) {
            this.seed = prefix + UNIQUE_STUB + (instanceCount++) + ":";
        }
    }

    public IdGenerator() {
        this("ID:" + hostName);
    }

    /**
     * As we have to find the hostname as a side-affect of generating a unique
     * stub, we allow it's easy retrevial here
     * 
     * @return the local host name
     */

    public static String getHostName() {
        return hostName;
    }


    /**
     * Generate a unqiue id
     * 
     * @return a unique id
     */

    public synchronized String generateId() {
        return this.seed + (this.sequence++);
    }

    /**
     * Generate a unique ID - that is friendly for a URL or file system
     * 
     * @return a unique id
     */
    public String generateSanitizedId() {
        String result = generateId();
        result = result.replace(':', '-');
        result = result.replace('_', '-');
        result = result.replace('.', '-');
        return result;
    }

    /**
     * From a generated id - return the seed (i.e. minus the count)
     * 
     * @param id the generated identifer
     * @return the seed
     */
    public static String getSeedFromId(String id) {
        String result = id;
        if (id != null) {
            int index = id.lastIndexOf(':');
            if (index > 0 && (index + 1) < id.length()) {
                result = id.substring(0, index + 1);
            }
        }
        return result;
    }

    /**
     * From a generated id - return the generator count
     * 
     * @param id
     * @return the count
     */
    public static long getSequenceFromId(String id) {
        long result = -1;
        if (id != null) {
            int index = id.lastIndexOf(':');

            if (index > 0 && (index + 1) < id.length()) {
                String numStr = id.substring(index + 1, id.length());
                result = Long.parseLong(numStr);
            }
        }
        return result;
    }

    /**
     * Does a proper compare on the ids
     * 
     * @param id1
     * @param id2
     * @return 0 if equal else a positive if id1 is > id2 ...
     */

    public static int compare(String id1, String id2) {
        int result = -1;
        String seed1 = IdGenerator.getSeedFromId(id1);
        String seed2 = IdGenerator.getSeedFromId(id2);
        if (seed1 != null && seed2 != null) {
            result = seed1.compareTo(seed2);
            if (result == 0) {
                long count1 = IdGenerator.getSequenceFromId(id1);
                long count2 = IdGenerator.getSequenceFromId(id2);
                result = (int)(count1 - count2);
            }
        }
        return result;

    }

}
