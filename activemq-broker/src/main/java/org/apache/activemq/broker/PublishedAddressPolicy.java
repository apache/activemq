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
package org.apache.activemq.broker;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.activemq.util.InetAddressUtil;

/**
 * Policy object that controls how a TransportConnector publishes the connector's
 * address to the outside world.  By default the connector will publish itself
 * using the resolved host name of the bound server socket.
 *
 * @org.apache.xbean.XBean
 */
public class PublishedAddressPolicy {

    private String clusterClientUriQuery;
    private PublishedHostStrategy publishedHostStrategy = PublishedHostStrategy.DEFAULT;
    private Map<Integer, Integer> portMapping = new HashMap<Integer, Integer>();
    private Map<String, String> hostMapping = new HashMap<String, String>();

    /**
     * Defines the value of the published host value.
     */
    public enum PublishedHostStrategy {
        DEFAULT,
        IPADDRESS,
        HOSTNAME,
        FQDN;

        public static PublishedHostStrategy getValue(String value) {
            return valueOf(value.toUpperCase(Locale.ENGLISH));
        }
    }

    /**
     * Using the supplied TransportConnector this method returns the String that will
     * be used to update clients with this connector's connect address.
     *
     * @param connector
     *      The TransportConnector whose address is to be published.
     * @return a string URI address that a client can use to connect to this Transport.
     * @throws Exception
     */
    public URI getPublishableConnectURI(TransportConnector connector) throws Exception {

        URI connectorURI = connector.getConnectUri();

        if (connectorURI == null) {
            return null;
        }

        String scheme = connectorURI.getScheme();
        if ("vm".equals(scheme)) {
            return connectorURI;
        }

        String userInfo = getPublishedUserInfoValue(connectorURI.getUserInfo());
        String host = getPublishedHostValue(connectorURI.getHost());
        if (hostMapping.containsKey(host)) {
            host = hostMapping.get(host);
        }

        int port = connectorURI.getPort();
        if (portMapping.containsKey(port)) {
            port = portMapping.get(port);
        }
        String path = getPublishedPathValue(connectorURI.getPath());
        String fragment = getPublishedFragmentValue(connectorURI.getFragment());

        URI publishedURI = new URI(scheme, userInfo, host, port, path, getClusterClientUriQuery(), fragment);
        return publishedURI;
    }

    public String getPublishableConnectString(TransportConnector connector) throws Exception {
        return getPublishableConnectURI(connector).toString();
    }

    /**
     * Subclasses can override what host value is published by implementing alternate
     * logic for this method.
     *
     * @param uriHostEntry
     *
     * @return the value published for the given host.
     *
     * @throws UnknownHostException
     */
    protected String getPublishedHostValue(String uriHostEntry) throws UnknownHostException {

        // By default we just republish what was already present.
        String result = uriHostEntry;

        if (this.publishedHostStrategy.equals(PublishedHostStrategy.IPADDRESS)) {
            InetAddress address = InetAddress.getByName(uriHostEntry);
            result = address.getHostAddress();
        } else if (this.publishedHostStrategy.equals(PublishedHostStrategy.HOSTNAME)) {
            InetAddress address = InetAddress.getByName(uriHostEntry);
            if (address.isAnyLocalAddress()) {
                // make it more human readable and useful, an alternative to 0.0.0.0
                result = InetAddressUtil.getLocalHostName();
            } else {
                result = address.getHostName();
            }
        } else if (this.publishedHostStrategy.equals(PublishedHostStrategy.FQDN)) {
            InetAddress address = InetAddress.getByName(uriHostEntry);
            if (address.isAnyLocalAddress()) {
                // make it more human readable and useful, an alternative to 0.0.0.0
                result = InetAddressUtil.getLocalHostName();
            } else {
                result = address.getCanonicalHostName();
            }
        }

        return result;
    }

    /**
     * Subclasses can override what path value is published by implementing alternate
     * logic for this method.  By default this method simply returns what was already
     * set as the Path value in the original URI.
     *
     * @param uriPathEntry
     *      The original value of the URI path.
     *
     * @return the desired value for the published URI's path.
     */
    protected String getPublishedPathValue(String uriPathEntry) {
        return uriPathEntry;
    }

    /**
     * Subclasses can override what host value is published by implementing alternate
     * logic for this method.  By default this method simply returns what was already
     * set as the Fragment value in the original URI.
     *
     * @param uriFragmentEntry
     *      The original value of the URI Fragment.
     *
     * @return the desired value for the published URI's Fragment.
     */
    protected String getPublishedFragmentValue(String uriFragmentEntry) {
        return uriFragmentEntry;
    }

    /**
     * Subclasses can override what user info value is published by implementing alternate
     * logic for this method.  By default this method simply returns what was already
     * set as the UserInfo value in the original URI.
     *
     * @param uriUserInfoEntry
     *      The original value of the URI user info.
     *
     * @return the desired value for the published URI's user info.
     */
    protected String getPublishedUserInfoValue(String uriUserInfoEntry) {
        return uriUserInfoEntry;
    }

    /**
     * Gets the URI query that's configured on the published URI that's sent to client's
     * when the cluster info is updated.
     *
     * @return the clusterClientUriQuery
     */
    public String getClusterClientUriQuery() {
        return clusterClientUriQuery;
    }

    /**
     * Sets the URI query that's configured on the published URI that's sent to client's
     * when the cluster info is updated.
     *
     * @param clusterClientUriQuery the clusterClientUriQuery to set
     */
    public void setClusterClientUriQuery(String clusterClientUriQuery) {
        this.clusterClientUriQuery = clusterClientUriQuery;
    }

    /**
     * @return the publishedHostStrategy
     */
    public PublishedHostStrategy getPublishedHostStrategy() {
        return publishedHostStrategy;
    }

    /**
     * @param strategy the publishedHostStrategy to set
     */
    public void setPublishedHostStrategy(PublishedHostStrategy strategy) {
        this.publishedHostStrategy = strategy;
    }

    /**
     * @param strategy the publishedHostStrategy to set
     */
    public void setPublishedHostStrategy(String strategy) {
        this.publishedHostStrategy = PublishedHostStrategy.getValue(strategy);
    }

    /**
     * @param portMapping map the ports in restrictive environments
     */
    public void setPortMapping(Map<Integer, Integer> portMapping) {
        this.portMapping = portMapping;
    }

    public Map<Integer, Integer>  getPortMapping() {
        return this.portMapping;
    }

    /**
     * @param hostMapping map the resolved hosts
     */
    public void setHostMapping(Map<String, String> hostMapping) {
        this.hostMapping = hostMapping;
    }

    public Map<String, String> getHostMapping() {
        return hostMapping;
    }
}
