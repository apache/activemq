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
package org.apache.activemq.console.filter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

public class MBeansAttributeQueryFilter extends AbstractQueryFilter {
    public static final String KEY_OBJECT_NAME_ATTRIBUTE = "Attribute:ObjectName:";

    private JMXServiceURL jmxServiceUrl;
    private Set attribView;

    /**
     * Create an mbean attributes query filter that is able to select specific
     * mbean attributes based on the object name to get.
     * 
     * @param jmxServiceUrl - JMX service url to connect to.
     * @param attribView - the attributes to extract
     * @param next - the next query filter
     */
    public MBeansAttributeQueryFilter(JMXServiceURL jmxServiceUrl, Set attribView, MBeansObjectNameQueryFilter next) {
        super(next);
        this.jmxServiceUrl = jmxServiceUrl;
        this.attribView = attribView;
    }

    /**
     * Filter the query by retrieving the attributes specified, this will modify
     * the collection to a list of AttributeList
     * 
     * @param queries - query list
     * @return List of AttributeList, which includes the ObjectName, which has a
     *         key of MBeansAttributeQueryFilter.KEY_OBJECT_NAME_ATTRIBUTE
     * @throws Exception
     */
    public List query(List queries) throws Exception {
        return getMBeanAttributesCollection(next.query(queries));
    }

    /**
     * Retrieve the specified attributes of the mbean
     * 
     * @param result - collection of ObjectInstances and/or ObjectNames
     * @return List of AttributeList
     * @throws IOException
     * @throws ReflectionException
     * @throws InstanceNotFoundException
     * @throws NoSuchMethodException
     */
    protected List getMBeanAttributesCollection(Collection result) throws IOException, ReflectionException, InstanceNotFoundException, NoSuchMethodException, IntrospectionException {
        List mbeansCollection = new ArrayList();

        for (Iterator i = result.iterator(); i.hasNext();) {
            Object mbean = i.next();
            if (mbean instanceof ObjectInstance) {
                mbeansCollection.add(getMBeanAttributes(((ObjectInstance)mbean).getObjectName(), attribView));
            } else if (mbean instanceof ObjectName) {
                mbeansCollection.add(getMBeanAttributes((ObjectName)mbean, attribView));
            } else {
                throw new NoSuchMethodException("Cannot get the mbean attributes for class: " + mbean.getClass().getName());
            }
        }

        return mbeansCollection;
    }

    /**
     * Retrieve the specified attributes of the mbean
     * 
     * @param obj - mbean ObjectInstance
     * @param attrView - list of attributes to retrieve
     * @return AttributeList for the mbean
     * @throws ReflectionException
     * @throws InstanceNotFoundException
     * @throws IOException
     */
    protected AttributeList getMBeanAttributes(ObjectInstance obj, Set attrView) throws ReflectionException, InstanceNotFoundException, IOException, IntrospectionException {
        return getMBeanAttributes(obj.getObjectName(), attrView);
    }

    /**
     * Retrieve the specified attributes of the mbean
     * 
     * @param objName - mbean ObjectName
     * @param attrView - list of attributes to retrieve
     * @return AttributeList for the mbean
     * @throws IOException
     * @throws ReflectionException
     * @throws InstanceNotFoundException
     */
    protected AttributeList getMBeanAttributes(ObjectName objName, Set attrView) throws IOException, ReflectionException, InstanceNotFoundException, IntrospectionException {
        JMXConnector jmxConnector = JMXConnectorFactory.connect(jmxServiceUrl);
        MBeanServerConnection server = jmxConnector.getMBeanServerConnection();

        // If no attribute view specified, get all attributes
        String[] attribs;
        if (attrView == null || attrView.isEmpty()) {
            MBeanAttributeInfo[] infos = server.getMBeanInfo(objName).getAttributes();
            attribs = new String[infos.length];

            for (int i = 0; i < infos.length; i++) {
                if (infos[i].isReadable()) {
                    attribs[i] = infos[i].getName();
                }
            }

            // Get selected attributes
        } else {

            attribs = new String[attrView.size()];
            int count = 0;
            for (Iterator i = attrView.iterator(); i.hasNext();) {
                attribs[count++] = (String)i.next();
            }
        }

        AttributeList attribList = server.getAttributes(objName, attribs);

        jmxConnector.close();

        attribList.add(0, new Attribute(KEY_OBJECT_NAME_ATTRIBUTE, objName));

        return attribList;
    }
}
