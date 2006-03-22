/**
 *
 * Copyright 2004 Protique Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **/
package org.activemq.gbean.management;

import java.util.Set;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Hashtable;
import javax.management.ObjectName;
import javax.management.MalformedObjectNameException;
import org.activemq.gbean.ActiveMQManager;
import org.activemq.gbean.ActiveMQBroker;
import org.activemq.gbean.ActiveMQConnector;
import org.activemq.gbean.ActiveMQConnectorGBean;
import org.apache.geronimo.gbean.GBeanInfo;
import org.apache.geronimo.gbean.GBeanInfoBuilder;
import org.apache.geronimo.gbean.GBeanQuery;
import org.apache.geronimo.gbean.GBeanData;
import org.apache.geronimo.kernel.Kernel;
import org.apache.geronimo.kernel.GBeanNotFoundException;
import org.apache.geronimo.j2ee.management.impl.Util;
import org.apache.geronimo.j2ee.j2eeobjectnames.NameFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Implementation of the ActiveMQ management interface.  These are the ActiveMQ
 * mangement features available at runtime.
 *
 * @version $Revision: 1.0$
 */
public class ActiveMQManagerGBean implements ActiveMQManager {
    private static final Log log = LogFactory.getLog(ActiveMQManagerGBean.class.getName());
    private Kernel kernel;

    public ActiveMQManagerGBean(Kernel kernel) {
        this.kernel = kernel;
    }

    public String[] getContainers() {
        GBeanQuery query = new GBeanQuery(null, ActiveMQBroker.class.getName());
        Set set = kernel.listGBeans(query);
        String[] results = new String[set.size()];
        int i=0;
        for (Iterator it = set.iterator(); it.hasNext();) {
            ObjectName name = (ObjectName) it.next();
            results[i++] = name.getCanonicalName();
        }
        return results;
    }

    public String[] getSupportedProtocols() {
        // see files in modules/core/src/conf/META-INF/services/org/activemq/transport/server/
        return new String[]{"activeio","jabber","multicast","openwire","peer","stomp","tcp","udp","vm",};
    }

    public String[] getConnectors() {
        GBeanQuery query = new GBeanQuery(null, ActiveMQConnector.class.getName());
        Set set = kernel.listGBeans(query);
        String[] results = new String[set.size()];
        int i=0;
        for (Iterator it = set.iterator(); it.hasNext();) {
            ObjectName name = (ObjectName) it.next();
            results[i++] = name.getCanonicalName();
        }
        return results;
    }

    public String[] getConnectors(String protocol) {
        if(protocol == null) {
            return getConnectors();
        }
        GBeanQuery query = new GBeanQuery(null, ActiveMQConnector.class.getName());
        Set set = kernel.listGBeans(query);
        List results = new ArrayList();
        for (Iterator it = set.iterator(); it.hasNext();) {
            ObjectName name = (ObjectName) it.next();
            try {
                String target = (String) kernel.getAttribute(name, "protocol");
                if(target != null && target.equals(protocol)) {
                    results.add(name.getCanonicalName());
                }
            } catch (Exception e) {
                log.error("Unable to look up protocol for connector '"+name+"'",e);
            }
        }
        return (String[]) results.toArray(new String[results.size()]);
    }

    public String[] getConnectorsForContainer(String broker) {
        try {
            ObjectName brokerName = ObjectName.getInstance(broker);
            List results = new ArrayList();
            GBeanQuery query = new GBeanQuery(null, ActiveMQConnector.class.getName());
            Set set = kernel.listGBeans(query); // all ActiveMQ connectors
            for (Iterator it = set.iterator(); it.hasNext();) {
                ObjectName name = (ObjectName) it.next(); // a single ActiveMQ connector
                GBeanData data = kernel.getGBeanData(name);
                Set refs = data.getReferencePatterns("activeMQContainer");
                for (Iterator refit = refs.iterator(); refit.hasNext();) {
                    ObjectName ref = (ObjectName) refit.next();
                    if(ref.isPattern()) {
                        Set matches = kernel.listGBeans(ref);
                        if(matches.size() != 1) {
                            log.error("Unable to compare a connector->container reference that's a pattern to a fixed container name: "+ref.getCanonicalName());
                        } else {
                            ref = (ObjectName)matches.iterator().next();
                            if(ref.equals(brokerName)) {
                                results.add(name.getCanonicalName());
                                break;
                            }
                        }
                    } else {
                        if(ref.equals(brokerName)) {
                            results.add(name.getCanonicalName());
                            break;
                        }
                    }
                }
            }
            return (String[]) results.toArray(new String[results.size()]);
        } catch (Exception e) {
            throw new IllegalArgumentException("Unable to look up connectors for broker '"+broker+"': "+e);
        }
    }

    public String[] getConnectorsForContainer(String broker, String protocol) {
        if(protocol == null) {
            return getConnectorsForContainer(broker);
        }
        try {
            ObjectName brokerName = ObjectName.getInstance(broker);
            List results = new ArrayList();
            GBeanQuery query = new GBeanQuery(null, ActiveMQConnector.class.getName());
            Set set = kernel.listGBeans(query); // all ActiveMQ connectors
            for (Iterator it = set.iterator(); it.hasNext();) {
                ObjectName name = (ObjectName) it.next(); // a single ActiveMQ connector
                GBeanData data = kernel.getGBeanData(name);
                Set refs = data.getReferencePatterns("activeMQContainer");
                for (Iterator refit = refs.iterator(); refit.hasNext();) {
                    ObjectName ref = (ObjectName) refit.next();
                    boolean match = false;
                    if(ref.isPattern()) {
                        Set matches = kernel.listGBeans(ref);
                        if(matches.size() != 1) {
                            log.error("Unable to compare a connector->container reference that's a pattern to a fixed container name: "+ref.getCanonicalName());
                        } else {
                            ref = (ObjectName)matches.iterator().next();
                            if(ref.equals(brokerName)) {
                                match = true;
                            }
                        }
                    } else {
                        if(ref.equals(brokerName)) {
                            match = true;
                        }
                    }
                    if(match) {
                        try {
                            String testProtocol = (String) kernel.getAttribute(name, "protocol");
                            if(testProtocol != null && testProtocol.equals(protocol)) {
                                results.add(name.getCanonicalName());
                            }
                        } catch (Exception e) {
                            log.error("Unable to look up protocol for connector '"+name+"'",e);
                        }
                        break;
                    }
                }
            }
            return (String[]) results.toArray(new String[results.size()]);
        } catch (Exception e) {
            throw new IllegalArgumentException("Unable to look up connectors for broker '"+broker+"': "+e);
        }
    }

    /**
     * Creates a new connector, and returns the ObjectName for it.  Note that
     * the connector may well require further customization before being fully
     * functional (e.g. SSL settings for a secure connector).
     */
    public String addConnector(String broker, String uniqueName, String protocol, String host, int port) {
        ObjectName brokerName = null;
        try {
            brokerName = ObjectName.getInstance(broker);
        } catch (MalformedObjectNameException e) {
            throw new IllegalArgumentException("Unable to parse ObjectName '"+broker+"'");
        }
        ObjectName name = getConnectorName(brokerName, protocol, host, port, uniqueName);
        GBeanData connector = new GBeanData(name, ActiveMQConnectorGBean.GBEAN_INFO);
        //todo: if SSL is supported, need to add more properties or use a different GBean?
        connector.setAttribute("protocol", protocol);
        connector.setAttribute("host", host);
        connector.setAttribute("port", new Integer(port));
        connector.setReferencePattern("activeMQContainer", brokerName);
        ObjectName config = Util.getConfiguration(kernel, brokerName);
        try {
            kernel.invoke(config, "addGBean", new Object[]{connector, Boolean.FALSE}, new String[]{GBeanData.class.getName(), boolean.class.getName()});
        } catch (Exception e) {
            log.error("Unable to add GBean ", e);
            return null;
        }
        return name.getCanonicalName();
    }

    public void removeConnector(String objectName) {
        ObjectName name = null;
        try {
            name = ObjectName.getInstance(objectName);
        } catch (MalformedObjectNameException e) {
            throw new IllegalArgumentException("Invalid object name '" + objectName + "': " + e.getMessage());
        }
        try {
            GBeanInfo info = kernel.getGBeanInfo(name);
            boolean found = false;
            Set intfs = info.getInterfaces();
            for (Iterator it = intfs.iterator(); it.hasNext();) {
                String intf = (String) it.next();
                if (intf.equals(ActiveMQConnector.class.getName())) {
                    found = true;
                }
            }
            if (!found) {
                throw new GBeanNotFoundException(name);
            }
            ObjectName config = Util.getConfiguration(kernel, name);
            kernel.invoke(config, "removeGBean", new Object[]{name}, new String[]{ObjectName.class.getName()});
        } catch (GBeanNotFoundException e) {
            log.warn("No such GBean '" + objectName + "'"); //todo: what if we want to remove a failed GBean?
        } catch (Exception e) {
            log.error("Unable to remove GBean", e);
        }
    }

    /**
     * Generate an ObjectName for a new connector GBean
     */
    private ObjectName getConnectorName(ObjectName broker, String protocol, String host, int port, String uniqueName) {
        Hashtable table = new Hashtable();
        table.put(NameFactory.J2EE_APPLICATION, broker.getKeyProperty(NameFactory.J2EE_APPLICATION));
        table.put(NameFactory.J2EE_SERVER, broker.getKeyProperty(NameFactory.J2EE_SERVER));
        table.put(NameFactory.J2EE_MODULE, broker.getKeyProperty(NameFactory.J2EE_MODULE));
        table.put(NameFactory.J2EE_TYPE, ActiveMQConnector.CONNECTOR_J2EE_TYPE);
        String brokerName = broker.getKeyProperty(NameFactory.J2EE_NAME);
        table.put("broker", brokerName);
        table.put(NameFactory.J2EE_NAME, brokerName+"."+protocol+"."+host+(port > -1 ? "."+port : "")+"-"+uniqueName);
        try {
            return ObjectName.getInstance(broker.getDomain(), table);
        } catch (MalformedObjectNameException e) {
            throw new IllegalStateException("Never should have failed: " + e.getMessage());
        }
    }

    public static final GBeanInfo GBEAN_INFO;

    static {
        GBeanInfoBuilder infoFactory = new GBeanInfoBuilder("ActiveMQ Manager", ActiveMQManagerGBean.class);
        infoFactory.addAttribute("kernel", Kernel.class, false);
        infoFactory.addInterface(ActiveMQManager.class);
        infoFactory.setConstructor(new String[]{"kernel"});
        GBEAN_INFO = infoFactory.getBeanInfo();
    }

    public static GBeanInfo getGBeanInfo() {
        return GBEAN_INFO;
    }
}
