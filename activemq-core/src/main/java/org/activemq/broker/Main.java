/**
 * <a href="http://activemq.org">ActiveMQ: The Open Source Message Fabric</a>
 *
 * Copyright 2005 (C) LogicBlaze, Inc. http://www.logicblaze.com
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
package org.activemq.broker;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.JarURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Main class that can bootstrap a ActiveMQ Broker. Handles command line
 * argument parsing to set up the broker classpath and System properties.
 * 
 * @version $Revision$
 */
public class Main {

    private static final String BROKER_FACTORY_CLASS = "org.activemq.broker.BrokerFactory";
    private static File activeMQHome;
    private final ArrayList extensions = new ArrayList();
    private URI uri;
    private ClassLoader classLoader;

    public static void main(String[] args) throws Throwable {
        Main main = new Main();
        
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("-D")) {
                String key = args[i].substring(2);
                String value = "";
                int pos = key.indexOf("=");
                if (pos >= 0) {
                    value = key.substring(pos + 1);
                    key = key.substring(0, pos);
                }
                System.setProperty(key, value);
            } else if (args[i].equals("--extdir")) {
                if( !canUseExtdir() ) {
                    System.out.println("Extension directory feature not available due to the system classpath being able to load: "+BROKER_FACTORY_CLASS);
                    printUsage();
                    return;
                }
                i++;
                if (i >= args.length) {
                    System.out.println("Extension directory not specified.");
                    printUsage();
                    return;
                }

                File directory = new File(args[i]);
                if (!directory.isDirectory()) {
                    System.out.println("Extension directory specified is not valid directory: " + directory);
                    printUsage();
                    return;
                }
                main.addExtensionDirectory(directory);

            } else if (args[i].equals("--version")) {
                System.out.println();
                System.out.println("ActiveMQ " + main.getVersion());
                System.out.println("For help or more information please see: http://www.logicblaze.com");
                System.out.println();
                return;
            } else if (args[i].equals("-h") || args[i].equals("--help") || args[i].equals("-?")) {
                printUsage();
                return;
            } else {
                if (main.getUri() != null) {
                    System.out.println("Multiple configuration uris cannot be specified.");
                    printUsage();
                    return;
                }
                try {
                    main.setUri(new URI(args[i]));
                } catch (URISyntaxException e) {
                    System.out.println("Invalid broker configuration URI: " + args[i] + ", reason: " + e.getMessage());
                    printUsage();
                    return;
                }
            }
        }
        
        // Add the default directories.
        if( canUseExtdir() ) {
            main.addExtensionDirectory(new File(main.getActiveMQHome(), "conf"));
            main.addExtensionDirectory(new File(main.getActiveMQHome(), "lib"));
            main.addExtensionDirectory(new File(new File(main.getActiveMQHome(), "lib"), "optional"));
        }
        

        if (main.getUri() == null) {
            main.setUri(getDefaultUri());
        }
        main.run();
    }


    public static URI getDefaultUri() {
        try {
            return new URI("xbean:activemq.xml");
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * The extension directory feature will not work if the broker factory is already in the classpath 
     * since we have to load him from a child ClassLoader we build for it to work correctly.
     * 
     * @return
     */
    public static boolean canUseExtdir() {
        try {
            Main.class.getClassLoader().loadClass(BROKER_FACTORY_CLASS);
            return false;
        } catch (ClassNotFoundException e) {
            return true;
        }
    }
    
    private static void printUsage() {
        System.out.println();
        System.out.println("Usage: Main [options] uri");
        System.out.println();
        System.out.println("Options:");
        if( canUseExtdir() ) {
        System.out.println(" --extdir dir   Add the jar files in the directory to the classpath.");
        }
        System.out.println(" -Dname=value   Define a system property");
        System.out.println(" --version      Display version information");
        System.out.println(" -h,-?,--help   Display help information");
        System.out.println();
        System.out.println("URI:");
        System.out.println();
        System.out.println(" XBean based broker configuration:");
        System.out.println("    ");
        System.out.println();
        System.out.println("    Example: Main xbean:file:activemq.xml");
        System.out.println("        Loads the xbean configuration file from the current working directory");
        System.out.println("    Example: Main xbean:activemq.xml");
        System.out.println("        Loads the xbean configuration file from the classpath");
        System.out.println();
        System.out.println(" URI Parameter based broker configuration:");
        System.out.println("    Example: Main broker:(tcp://localhost:61616, tcp://localhost:5000)?useJmx=true");
        System.out.println("        Configures the broker with 2 transport connectors and jmx enabled");
        System.out
                .println("    Example: Main broker:(tcp://localhost:61616, network:tcp://localhost:5000)?persistent=false");
        System.out
                .println("        Configures the broker with 1 transport connector, and 1 network connector and persistence disabled");
        System.out.println();
    }

    public String getVersion() throws Throwable {
        ClassLoader cl = getClassLoader();
        // Use reflection to get teh version
        Object broker;
        try {
            Class activeMQConnectionMetaData = cl.loadClass("org.activemq.ActiveMQConnectionMetaData");
            Field field = activeMQConnectionMetaData.getField("PROVIDER_VERSION");
            return (String)field.get(null);
        } catch (Throwable e) {
            throw e;
        }
    }

    /**
     * @throws Throwable
     * 
     */
    public void run() throws Throwable {

        System.out.println("Loading Message Broker from: " + uri);
        System.out.println("ACTIVEMQ_HOME: "+getActiveMQHome());

        ClassLoader cl = getClassLoader();

        // Use reflection to start the broker up.
        Object broker;
        try {
            
            Class brokerFactory = cl.loadClass(BROKER_FACTORY_CLASS);
            Method createBroker = brokerFactory.getMethod("createBroker", new Class[] { URI.class });
            broker = createBroker.invoke(null, new Object[] { uri });
            
            Method start = broker.getClass().getMethod("start", new Class[]{});
            start.invoke(broker, new Object[]{});
            
        } catch (InvocationTargetException e) {
            throw e.getCause();
        } catch (Throwable e) {
            throw e;
        }

        final boolean[] shutdown = new boolean[]{false};
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                synchronized(shutdown) {
                    shutdown[0]=true;
                    shutdown.notify();
                }
            }
        });
        synchronized(shutdown) {
            while( !shutdown[0] ) {
                shutdown.wait();
            }
        }
        
        // Use reflection to stop the broker
        try {
            Method stop = broker.getClass().getMethod("stop", new Class[] {});
            stop.invoke(broker, new Object[] {});
        } catch (InvocationTargetException e) {
            throw e.getCause();
        } catch (Throwable e) {
            throw e;
        }
    }


    /**
     * @return
     * @throws MalformedURLException
     */
    public ClassLoader getClassLoader() throws MalformedURLException {
        if( classLoader==null ) {
            
            //
            // Setup the ClassLoader
            //
            classLoader = Main.class.getClassLoader();
            if (!extensions.isEmpty()) {
    
                ArrayList urls = new ArrayList();
                for (Iterator iter = extensions.iterator(); iter.hasNext();) {
                    File dir = (File) iter.next();
                    urls.add(dir.toURL());
                    File[] files = dir.listFiles();
                    if( files!=null ) {
                        for (int j = 0; j < files.length; j++) {
                            if( files[j].getName().endsWith(".zip") || files[j].getName().endsWith(".jar") ) {
                                urls.add(files[j].toURL());
                            }
                        }
                    }
                }
    
                URL u[] = new URL[urls.size()];
                urls.toArray(u);
                classLoader = new URLClassLoader(u, classLoader);
            }
            
            Thread.currentThread().setContextClassLoader(classLoader);
        }
        return classLoader;
    }

    public URI getUri() {
        return uri;
    }

    public void setUri(URI config) {
        this.uri = config;
    }

    public void addExtensionDirectory(File directory) {
        extensions.add(directory);
    }

    public File getActiveMQHome() {
        if( activeMQHome==null ) {
            if( System.getProperty("activemq.home") != null ) {
                activeMQHome = new File(System.getProperty("activemq.home"));
            }
            if( activeMQHome==null ){
                // guess from the location of the jar
                URL url = Main.class.getClassLoader().getResource("org/activemq/broker/Main.class");
                if (url != null) {
                    try {
                        JarURLConnection jarConnection = (JarURLConnection) url.openConnection();
                        url = jarConnection.getJarFileURL();
                        URI baseURI = new URI(url.toString()).resolve("..");
                        activeMQHome = new File(baseURI).getCanonicalFile();
                    } catch (Exception ignored) {
                    }
                }
            }
            if(activeMQHome==null){
                activeMQHome = new File(".");
            }
        }
        return activeMQHome;
    }

    public static void setActiveMQHome(File activeMQHome) {
        Main.activeMQHome = activeMQHome;
    }
}
