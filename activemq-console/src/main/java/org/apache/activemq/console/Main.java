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
package org.apache.activemq.console;

import java.io.File;
import java.io.InputStream;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.JarURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * Main class that can bootstrap an ActiveMQ broker console. Handles command
 * line argument parsing to set up and run broker tasks.
 */
public class Main {

    public static final String TASK_DEFAULT_CLASS = "org.apache.activemq.console.command.ShellCommand";
    private static boolean useDefExt = true;

    private File activeMQHome;
    private File activeMQBase;
    private ClassLoader classLoader;
    private final Set<File> extensions = new LinkedHashSet<File>();
    private final Set<File> activeMQClassPath = new LinkedHashSet<File>();

    public static void main(String[] args) {

        // Create the tmpdir if it does not exist yet..
        File tmpdir = new File(System.getProperty("java.io.tmpdir"));
        if(!tmpdir.exists()) {
            tmpdir.mkdirs();
        }

        Main app = new Main();

        // Convert arguments to collection for easier management
        List<String> tokens = new LinkedList<String>(Arrays.asList(args));
        // Parse for extension directory option
        app.parseExtensions(tokens);

        // lets add the conf directory first, to find the log4j.properties just in case its not
        // in the activemq.classpath system property or some jar incorrectly includes one
        File confDir = app.getActiveMQConfig();
        app.addClassPath(confDir);

        // Add the following to the classpath:
        //
        // ${activemq.base}/conf
        // ${activemq.base}/lib/* (only if activemq.base != activemq.home)
        // ${activemq.home}/lib/*
        // ${activemq.base}/lib/optional/* (only if activemq.base !=
        // activemq.home)
        // ${activemq.home}/lib/optional/*
        // ${activemq.base}/lib/web/* (only if activemq.base != activemq.home)
        // ${activemq.home}/lib/web/*
        //
        if (useDefExt && app.canUseExtdir()) {

            boolean baseIsHome = app.getActiveMQBase().equals(app.getActiveMQHome());

            File baseLibDir = new File(app.getActiveMQBase(), "lib");
            File homeLibDir = new File(app.getActiveMQHome(), "lib");

            if (!baseIsHome) {
                app.addExtensionDirectory(baseLibDir);
            }
            app.addExtensionDirectory(homeLibDir);

            if (!baseIsHome) {
                app.addExtensionDirectory(new File(baseLibDir, "camel"));
                app.addExtensionDirectory(new File(baseLibDir, "optional"));
                app.addExtensionDirectory(new File(baseLibDir, "web"));
                app.addExtensionDirectory(new File(baseLibDir, "extra"));
            }
            app.addExtensionDirectory(new File(homeLibDir, "camel"));
            app.addExtensionDirectory(new File(homeLibDir, "optional"));
            app.addExtensionDirectory(new File(homeLibDir, "web"));
            app.addExtensionDirectory(new File(homeLibDir, "extra"));
        }

        // Add any custom classpath specified from the system property
        // activemq.classpath
        app.addClassPathList(System.getProperty("activemq.classpath"));

        try {
            int ret = app.runTaskClass(tokens);
            System.exit(ret);
        } catch (ClassNotFoundException e) {
            System.out.println("Could not load class: " + e.getMessage());
            try {
                ClassLoader cl = app.getClassLoader();
                if (cl != null) {
                    System.out.println("Class loader setup: ");
                    printClassLoaderTree(cl);
                }
            } catch (MalformedURLException e1) {
            }
            System.exit(1);
        } catch (Throwable e) {
            System.out.println("Failed to execute main task. Reason: " + e);
            System.exit(1);
        }
    }

    /**
     * Print out what's in the classloader tree being used.
     *
     * @param cl
     * @return depth
     */
    private static int printClassLoaderTree(ClassLoader cl) {
        int depth = 0;
        if (cl.getParent() != null) {
            depth = printClassLoaderTree(cl.getParent()) + 1;
        }

        StringBuffer indent = new StringBuffer();
        for (int i = 0; i < depth; i++) {
            indent.append("  ");
        }

        if (cl instanceof URLClassLoader) {
            URLClassLoader ucl = (URLClassLoader)cl;
            System.out.println(indent + cl.getClass().getName() + " {");
            URL[] urls = ucl.getURLs();
            for (int i = 0; i < urls.length; i++) {
                System.out.println(indent + "  " + urls[i]);
            }
            System.out.println(indent + "}");
        } else {
            System.out.println(indent + cl.getClass().getName());
        }
        return depth;
    }

    public void parseExtensions(List<String> tokens) {
        if (tokens.isEmpty()) {
            return;
        }

        int count = tokens.size();
        int i = 0;

        // Parse for all --extdir and --noDefExt options
        while (i < count) {
            String token = tokens.get(i);
            // If token is an extension dir option
            if (token.equals("--extdir")) {
                // Process token
                count--;
                tokens.remove(i);

                // If no extension directory is specified, or next token is
                // another option
                if (i >= count || tokens.get(i).startsWith("-")) {
                    System.out.println("Extension directory not specified.");
                    System.out.println("Ignoring extension directory option.");
                    continue;
                }

                // Process extension dir token
                count--;
                File extDir = new File(tokens.remove(i));

                if (!canUseExtdir()) {
                    System.out.println("Extension directory feature not available due to the system classpath being able to load: " + TASK_DEFAULT_CLASS);
                    System.out.println("Ignoring extension directory option.");
                    continue;
                }

                if (!extDir.isDirectory()) {
                    System.out.println("Extension directory specified is not valid directory: " + extDir);
                    System.out.println("Ignoring extension directory option.");
                    continue;
                }

                addExtensionDirectory(extDir);
            } else if (token.equals("--noDefExt")) { // If token is
                // --noDefExt option
                count--;
                tokens.remove(i);
                useDefExt = false;
            } else {
                i++;
            }
        }

    }

    public int runTaskClass(List<String> tokens) throws Throwable {

        StringBuilder buffer = new StringBuilder();
        buffer.append(System.getProperty("java.vendor"));
        buffer.append(" ");
        buffer.append(System.getProperty("java.version"));
        buffer.append(" ");
        buffer.append(System.getProperty("java.home"));
        System.out.println("Java Runtime: " + buffer.toString());

        buffer = new StringBuilder();
        buffer.append("current=");
        buffer.append(Runtime.getRuntime().totalMemory()/1024L);
        buffer.append("k  free=");
        buffer.append(Runtime.getRuntime().freeMemory()/1024L);
        buffer.append("k  max=");
        buffer.append(Runtime.getRuntime().maxMemory()/1024L);
        buffer.append("k");
        System.out.println("  Heap sizes: " + buffer.toString());

        List<?> jvmArgs = ManagementFactory.getRuntimeMXBean().getInputArguments();
        buffer = new StringBuilder();
        for (Object arg : jvmArgs) {
            buffer.append(" ").append(arg);
        }
        System.out.println("    JVM args:" + buffer.toString());
        System.out.println("Extensions classpath:\n  " + getExtensionDirForLogging());

        System.out.println("ACTIVEMQ_HOME: " + getActiveMQHome());
        System.out.println("ACTIVEMQ_BASE: " + getActiveMQBase());
        System.out.println("ACTIVEMQ_CONF: " + getActiveMQConfig());
        System.out.println("ACTIVEMQ_DATA: " + getActiveMQDataDir());

        ClassLoader cl = getClassLoader();
        Thread.currentThread().setContextClassLoader(cl);

        // Use reflection to run the task.
        try {
            String[] args = tokens.toArray(new String[tokens.size()]);
            Class<?> task = cl.loadClass(TASK_DEFAULT_CLASS);
            Method runTask = task.getMethod("main", new Class[] {
                String[].class, InputStream.class, PrintStream.class
            });
            return (int)runTask.invoke(task.newInstance(), args, System.in, System.out);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    public void addExtensionDirectory(File directory) {
        extensions.add(directory);
    }

    public void addClassPathList(String fileList) {
        if (fileList != null && fileList.length() > 0) {
            StringTokenizer tokenizer = new StringTokenizer(fileList, File.pathSeparator);
            while (tokenizer.hasMoreTokens()) {
                addClassPath(new File(tokenizer.nextToken()));
            }
        }
    }

    public void addClassPath(File classpath) {
        activeMQClassPath.add(classpath);
    }

    /**
     * The extension directory feature will not work if the broker factory is
     * already in the classpath since we have to load him from a child
     * ClassLoader we build for it to work correctly.
     *
     * @return true, if extension dir can be used. false otherwise.
     */
    public boolean canUseExtdir() {
        try {
            Main.class.getClassLoader().loadClass(TASK_DEFAULT_CLASS);
            return false;
        } catch (ClassNotFoundException e) {
            return true;
        }
    }

    public ClassLoader getClassLoader() throws MalformedURLException {
        if (classLoader == null) {
            // Setup the ClassLoader
            classLoader = Main.class.getClassLoader();
            if (!extensions.isEmpty() || !activeMQClassPath.isEmpty()) {

                ArrayList<URL> urls = new ArrayList<URL>();

                for (Iterator<File> iter = activeMQClassPath.iterator(); iter.hasNext();) {
                    File dir = iter.next();
                    urls.add(dir.toURI().toURL());
                }

                for (Iterator<File> iter = extensions.iterator(); iter.hasNext();) {
                    File dir = iter.next();
                    if (dir.isDirectory()) {
                        File[] files = dir.listFiles();
                        if (files != null) {

                            // Sort the jars so that classpath built is consistently in the same
                            // order. Also allows us to use jar names to control classpath order.
                            Arrays.sort(files, new Comparator<File>() {
                                @Override
                                public int compare(File f1, File f2) {
                                    return f1.getName().compareTo(f2.getName());
                                }
                            });

                            for (int j = 0; j < files.length; j++) {
                                if (files[j].getName().endsWith(".zip") || files[j].getName().endsWith(".jar")) {
                                    urls.add(files[j].toURI().toURL());
                                }
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

    public void setActiveMQHome(File activeMQHome) {
        this.activeMQHome = activeMQHome;
    }

    public File getActiveMQHome() {
        if (activeMQHome == null) {
            if (System.getProperty("activemq.home") != null) {
                activeMQHome = new File(System.getProperty("activemq.home"));
            }

            if (activeMQHome == null) {
                // guess from the location of the jar
                URL url = Main.class.getClassLoader().getResource("org/apache/activemq/console/Main.class");
                if (url != null) {
                    try {
                        JarURLConnection jarConnection = (JarURLConnection)url.openConnection();
                        url = jarConnection.getJarFileURL();
                        URI baseURI = new URI(url.toString()).resolve("..");
                        activeMQHome = new File(baseURI).getCanonicalFile();
                        System.setProperty("activemq.home", activeMQHome.getAbsolutePath());
                    } catch (Exception ignored) {
                    }
                }
            }

            if (activeMQHome == null) {
                activeMQHome = new File("../.");
                System.setProperty("activemq.home", activeMQHome.getAbsolutePath());
            }
        }

        return activeMQHome;
    }

    public File getActiveMQBase() {
        if (activeMQBase == null) {
            if (System.getProperty("activemq.base") != null) {
                activeMQBase = new File(System.getProperty("activemq.base"));
            }

            if (activeMQBase == null) {
                activeMQBase = getActiveMQHome();
                System.setProperty("activemq.base", activeMQBase.getAbsolutePath());
            }
        }

        return activeMQBase;
    }

    public File getActiveMQConfig() {
        File activeMQConfig = null;

        if (System.getProperty("activemq.conf") != null) {
            activeMQConfig = new File(System.getProperty("activemq.conf"));
        } else {
            activeMQConfig = new File(getActiveMQBase() + "/conf");
            System.setProperty("activemq.conf", activeMQConfig.getAbsolutePath());
        }
        return activeMQConfig;
    }

    public File getActiveMQDataDir() {
        File activeMQDataDir = null;

        if (System.getProperty("activemq.data") != null) {
            activeMQDataDir = new File(System.getProperty("activemq.data"));
        } else {
            activeMQDataDir = new File(getActiveMQBase() + "/data");
            System.setProperty("activemq.data", activeMQDataDir.getAbsolutePath());
        }
        return activeMQDataDir;
    }

    public String getExtensionDirForLogging() {
        StringBuilder sb = new StringBuilder("[");
        for (Iterator<File> it = extensions.iterator(); it.hasNext();) {
            File file = it.next();
            sb.append(file.getPath());
            if (it.hasNext()) {
                sb.append(",");
            }
        }
        sb.append("]");
        return sb.toString();
    }
}
