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
package org.apache.activemq.jndi;

import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Properties;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.StringRefAddr;
import javax.naming.spi.ObjectFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts objects implementing JNDIStorable into a property fields so they can
 * be stored and regenerated from JNDI
 */
public class JNDIReferenceFactory implements ObjectFactory {

    static Logger log = LoggerFactory.getLogger(JNDIReferenceFactory.class);

    /**
     * This will be called by a JNDIprovider when a Reference is retrieved from
     * a JNDI store - and generates the original instance
     * 
     * @param object the Reference object
     * @param name the JNDI name
     * @param nameCtx the context
     * @param environment the environment settings used by JNDI
     * @return the instance built from the Reference object
     * @throws Exception if building the instance from Reference fails (usually
     *                 class not found)
     */
    public Object getObjectInstance(Object object, Name name, Context nameCtx, Hashtable environment) throws Exception {
        Object result = null;
        if (object instanceof Reference) {
            Reference reference = (Reference)object;

            if (log.isTraceEnabled()) {
                log.trace("Getting instance of " + reference.getClassName());
            }

            Class theClass = loadClass(this, reference.getClassName());
            if (JNDIStorableInterface.class.isAssignableFrom(theClass)) {

                JNDIStorableInterface store = (JNDIStorableInterface)theClass.newInstance();
                Properties properties = new Properties();
                for (Enumeration iter = reference.getAll(); iter.hasMoreElements();) {

                    StringRefAddr addr = (StringRefAddr)iter.nextElement();
                    properties.put(addr.getType(), (addr.getContent() == null) ? "" : addr.getContent());

                }
                store.setProperties(properties);
                result = store;
            }
        } else {
            log.error("Object " + object + " is not a reference - cannot load");
            throw new RuntimeException("Object " + object + " is not a reference");
        }
        return result;
    }

    /**
     * Create a Reference instance from a JNDIStorable object
     * 
     * @param instanceClassName
     * @param po
     * @return
     * @throws NamingException
     */

    public static Reference createReference(String instanceClassName, JNDIStorableInterface po) throws NamingException {
        if (log.isTraceEnabled()) {
            log.trace("Creating reference: " + instanceClassName + "," + po);
        }
        Reference result = new Reference(instanceClassName, JNDIReferenceFactory.class.getName(), null);
        try {
            Properties props = po.getProperties();
            for (Enumeration iter = props.propertyNames(); iter.hasMoreElements();) {
                String key = (String)iter.nextElement();
                String value = props.getProperty(key);
                javax.naming.StringRefAddr addr = new javax.naming.StringRefAddr(key, value);
                result.add(addr);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new NamingException(e.getMessage());
        }
        return result;
    }

    /**
     * Retrieve the class loader for a named class
     * 
     * @param thisObj
     * @param className
     * @return
     * @throws ClassNotFoundException
     */

    public static Class loadClass(Object thisObj, String className) throws ClassNotFoundException {
        // tryu local ClassLoader first.
        ClassLoader loader = thisObj.getClass().getClassLoader();
        Class theClass;
        if (loader != null) {
            theClass = loader.loadClass(className);
        } else {
            // Will be null in jdk1.1.8
            // use default classLoader
            theClass = Class.forName(className);
        }
        return theClass;
    }

}
