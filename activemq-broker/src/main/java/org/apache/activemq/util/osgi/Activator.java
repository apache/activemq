/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.activemq.util.osgi;

import static org.osgi.framework.wiring.BundleRevision.PACKAGE_NAMESPACE;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.activemq.Service;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.discovery.DiscoveryAgent;
import org.apache.activemq.util.FactoryFinder;
import org.apache.activemq.util.FactoryFinder.ObjectFactory;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleEvent;
import org.osgi.framework.SynchronousBundleListener;
import org.osgi.framework.wiring.BundleCapability;
import org.osgi.framework.wiring.BundleWire;
import org.osgi.framework.wiring.BundleWiring;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An OSGi bundle activator for ActiveMQ which adapts the {@link org.apache.activemq.util.FactoryFinder}
 * to the OSGi environment.
 *
 */
public class Activator implements BundleActivator, SynchronousBundleListener, ObjectFactory {

    private static final Logger LOG = LoggerFactory.getLogger(Activator.class);

    private final ConcurrentMap<String, Class<?>> serviceCache = new ConcurrentHashMap<String, Class<?>>();
    private final ConcurrentMap<Long, BundleWrapper> bundleWrappers = new ConcurrentHashMap<Long, BundleWrapper>();
    private BundleContext bundleContext;
    private Set<BundleCapability> packageCapabilities = new HashSet<BundleCapability>();

    // ================================================================
    // BundleActivator interface impl
    // ================================================================

    @Override
    public synchronized void start(BundleContext bundleContext) throws Exception {

        // This is how we replace the default FactoryFinder strategy
        // with one that is more compatible in an OSGi env.
        FactoryFinder.setObjectFactory(this);

        debug("activating");
        this.bundleContext = bundleContext;

        cachePackageCapabilities(Service.class, Transport.class, DiscoveryAgent.class, PersistenceAdapter.class);

        debug("checking existing bundles");
        bundleContext.addBundleListener(this);
        for (Bundle bundle : bundleContext.getBundles()) {
            if (bundle.getState() == Bundle.RESOLVED || bundle.getState() == Bundle.STARTING ||
                bundle.getState() == Bundle.ACTIVE || bundle.getState() == Bundle.STOPPING) {
                register(bundle);
            }
        }
        debug("activated");
    }

    /**
     * Caches the package capabilities that are needed for a set of interface classes
     *
     * @param classes interfaces we want to track
     */
    private void cachePackageCapabilities(Class<?> ... classes) {
        BundleWiring ourWiring = bundleContext.getBundle().adapt(BundleWiring.class);
        Set<String> packageNames = new HashSet<String>();
        for (Class<?> clazz: classes) {
            packageNames.add(clazz.getPackage().getName());
        }

        List<BundleCapability> ourExports = ourWiring.getCapabilities(PACKAGE_NAMESPACE);
        for (BundleCapability ourExport : ourExports) {
            String ourPkgName = (String) ourExport.getAttributes().get(PACKAGE_NAMESPACE);
            if (packageNames.contains(ourPkgName)) {
                packageCapabilities.add(ourExport);
            }
        }
    }


    @Override
    public synchronized void stop(BundleContext bundleContext) throws Exception {
        debug("deactivating");
        bundleContext.removeBundleListener(this);
        while (!bundleWrappers.isEmpty()) {
            unregister(bundleWrappers.keySet().iterator().next());
        }
        debug("deactivated");
        this.bundleContext = null;
    }

    // ================================================================
    // SynchronousBundleListener interface impl
    // ================================================================

    @Override
    public void bundleChanged(BundleEvent event) {
        if (event.getType() == BundleEvent.RESOLVED) {
            register(event.getBundle());
        } else if (event.getType() == BundleEvent.UNRESOLVED || event.getType() == BundleEvent.UNINSTALLED) {
            unregister(event.getBundle().getBundleId());
        }
    }

    protected void register(final Bundle bundle) {
        debug("checking bundle " + bundle.getBundleId());
        if (isOurBundle(bundle) || isImportingUs(bundle) ) {
            debug("Registering bundle for extension resolution: "+ bundle.getBundleId());
            bundleWrappers.put(bundle.getBundleId(), new BundleWrapper(bundle));
        }
    }

    private boolean isOurBundle(final Bundle bundle) {
        return bundle.getBundleId() == bundleContext.getBundle().getBundleId();
    }

    /**
     * When bundles unload.. we remove them thier cached Class entries from the
     * serviceCache.  Future service lookups for the service will fail.
     *
     * TODO: consider a way to get the Broker release any references to
     * instances of the service.
     *
     * @param bundleId
     */
    protected void unregister(long bundleId) {
        BundleWrapper bundle = bundleWrappers.remove(bundleId);
        if (bundle != null) {
            for (String path : bundle.cachedServices) {
                debug("unregistering service for key: " +path );
                serviceCache.remove(path);
            }
        }
    }

    // ================================================================
    // ObjectFactory interface impl
    // ================================================================

    @Override
    public Object create(String path) throws IllegalAccessException, InstantiationException, IOException, ClassNotFoundException {
        Class<?> clazz = serviceCache.get(path);
        if (clazz == null) {
            StringBuffer warnings = new StringBuffer();
            // We need to look for a bundle that has that class.
            int wrrningCounter=1;
            for (BundleWrapper wrapper : bundleWrappers.values()) {
                URL resource = wrapper.bundle.getResource(path);
                if( resource == null ) {
                    continue;
                }

                Properties properties = loadProperties(resource);

                String className = properties.getProperty("class");
                if (className == null) {
                    warnings.append("("+(wrrningCounter++)+") Invalid service file in bundle "+wrapper+": 'class' property not defined.");
                    continue;
                }

                try {
                    clazz = wrapper.bundle.loadClass(className);
                } catch (ClassNotFoundException e) {
                    warnings.append("("+(wrrningCounter++)+") Bundle "+wrapper+" could not load "+className+": "+e);
                    continue;
                }

                // Yay.. the class was found.  Now cache it.
                serviceCache.put(path, clazz);
                wrapper.cachedServices.add(path);
                break;
            }

            if( clazz == null ) {
                // Since OSGi is such a tricky environment to work in.. lets give folks the
                // most information we can in the error message.
                String msg = "Service not found: '" + path + "'";
                if (warnings.length()!= 0) {
                    msg += ", "+warnings;
                }
                throw new IOException(msg);
            }
        }
        return clazz.newInstance();
    }

    // ================================================================
    // Internal Helper Methods
    // ================================================================

    private void debug(Object msg) {
        LOG.debug(msg.toString());
    }

    private Properties loadProperties(URL resource) throws IOException {
        InputStream in = resource.openStream();
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(in, "UTF-8"));
            Properties properties = new Properties();
            properties.load(in);
            return properties;
        } finally {
            try {
                in.close();
            } catch (Exception e) {
            }
        }
    }

    /**
     * We consider a bundle to be a candidate for objects if it imports at least
     * one of the packages of our interfaces
     *
     * @param bundle
     * @return true if the bundle is improting.
     */
    private boolean isImportingUs(Bundle bundle) {
        BundleWiring wiring = bundle.adapt(BundleWiring.class);
        List<BundleWire> imports = wiring.getRequiredWires(PACKAGE_NAMESPACE);
        for (BundleWire importWire : imports) {
            if (packageCapabilities.contains(importWire.getCapability())) {
                return true;
            }
        }
        return false;
    }

    private static class BundleWrapper {
        private final Bundle bundle;
        private final List<String> cachedServices = new ArrayList<String>();

        public BundleWrapper(Bundle bundle) {
            this.bundle = bundle;
        }
    }
}
