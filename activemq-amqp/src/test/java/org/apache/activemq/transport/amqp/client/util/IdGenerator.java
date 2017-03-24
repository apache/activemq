/*
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
package org.apache.activemq.transport.amqp.client.util;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generator for Globally unique Strings.
 */
public class IdGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(IdGenerator.class);
    private static final String UNIQUE_STUB;
    private static int instanceCount;
    private static String hostName;
    private String seed;
    private final AtomicLong sequence = new AtomicLong(1);
    private int length;
    public static final String PROPERTY_IDGENERATOR_PORT = "activemq.idgenerator.port";

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
            int idGeneratorPort = 0;
            ServerSocket ss = null;
            try {
                idGeneratorPort = Integer.parseInt(System.getProperty(PROPERTY_IDGENERATOR_PORT, "0"));
                LOG.trace("Using port {}", idGeneratorPort);
                hostName = getLocalHostName();
                ss = new ServerSocket(idGeneratorPort);
                stub = "-" + ss.getLocalPort() + "-" + System.currentTimeMillis() + "-";
                Thread.sleep(100);
            } catch (Exception e) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("could not generate unique stub by using DNS and binding to local port", e);
                } else {
                    LOG.warn("could not generate unique stub by using DNS and binding to local port: {} {}", e.getClass().getCanonicalName(), e.getMessage());
                }

                // Restore interrupted state so higher level code can deal with it.
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
            } finally {
                if (ss != null) {
                    try {
                        ss.close();
                    } catch (IOException ioe) {
                        if (LOG.isTraceEnabled()) {
                            LOG.trace("Closing the server socket failed", ioe);
                        } else {
                            LOG.warn("Closing the server socket failed" + " due " + ioe.getMessage());
                        }
                    }
                }
            }
        }

        if (hostName == null) {
            hostName = "localhost";
        }
        hostName = sanitizeHostName(hostName);

        if (stub.length() == 0) {
            stub = "-1-" + System.currentTimeMillis() + "-";
        }
        UNIQUE_STUB = stub;
    }

    /**
     * Construct an IdGenerator
     *
     * @param prefix
     *      The prefix value that is applied to all generated IDs.
     */
    public IdGenerator(String prefix) {
        synchronized (UNIQUE_STUB) {
            this.seed = prefix + UNIQUE_STUB + (instanceCount++) + ":";
            this.length = this.seed.length() + ("" + Long.MAX_VALUE).length();
        }
    }

    public IdGenerator() {
        this("ID:" + hostName);
    }

    /**
     * As we have to find the host name as a side-affect of generating a unique stub, we allow
     * it's easy retrieval here
     *
     * @return the local host name
     */
    public static String getHostName() {
        return hostName;
    }

    /**
     * Generate a unique id
     *
     * @return a unique id
     */
    public synchronized String generateId() {
        StringBuilder sb = new StringBuilder(length);
        sb.append(seed);
        sb.append(sequence.getAndIncrement());
        return sb.toString();
    }

    public static String sanitizeHostName(String hostName) {
        boolean changed = false;

        StringBuilder sb = new StringBuilder();
        for (char ch : hostName.toCharArray()) {
            // only include ASCII chars
            if (ch < 127) {
                sb.append(ch);
            } else {
                changed = true;
            }
        }

        if (changed) {
            String newHost = sb.toString();
            LOG.info("Sanitized hostname from: {} to: {}", hostName, newHost);
            return newHost;
        } else {
            return hostName;
        }
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
     * @param id
     *        the generated identifier
     * @return the seed
     */
    public static String getSeedFromId(String id) {
        String result = id;
        if (id != null) {
            int index = id.lastIndexOf(':');
            if (index > 0 && (index + 1) < id.length()) {
                result = id.substring(0, index);
            }
        }
        return result;
    }

    /**
     * From a generated id - return the generator count
     *
     * @param id
     *      The ID that will be parsed for a sequence number.
     *
     * @return the sequence value parsed from the given ID.
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
     * Does a proper compare on the Id's
     *
     * @param id1 the lhs of the comparison.
     * @param id2 the rhs of the comparison.
     *
     * @return 0 if equal else a positive if {@literal id1 > id2} ...
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
                result = (int) (count1 - count2);
            }
        }
        return result;
    }

    /**
     * When using the {@link java.net.InetAddress#getHostName()} method in an
     * environment where neither a proper DNS lookup nor an <tt>/etc/hosts</tt>
     * entry exists for a given host, the following exception will be thrown:
     * <code>
     * java.net.UnknownHostException: &lt;hostname&gt;: &lt;hostname&gt;
     *  at java.net.InetAddress.getLocalHost(InetAddress.java:1425)
     *   ...
     * </code>
     * Instead of just throwing an UnknownHostException and giving up, this
     * method grabs a suitable hostname from the exception and prevents the
     * exception from being thrown. If a suitable hostname cannot be acquired
     * from the exception, only then is the <tt>UnknownHostException</tt> thrown.
     *
     * @return The hostname
     *
     * @throws UnknownHostException if the given host cannot be looked up.
     *
     * @see java.net.InetAddress#getLocalHost()
     * @see java.net.InetAddress#getHostName()
     */
    protected static String getLocalHostName() throws UnknownHostException {
        try {
            return (InetAddress.getLocalHost()).getHostName();
        } catch (UnknownHostException uhe) {
            String host = uhe.getMessage(); // host = "hostname: hostname"
            if (host != null) {
                int colon = host.indexOf(':');
                if (colon > 0) {
                    return host.substring(0, colon);
                }
            }
            throw uhe;
        }
    }
}
