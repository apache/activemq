/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
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
 */
package org.apache.activemq.broker;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.JarURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.LinkedList;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Main class that can bootstrap an ActiveMQ broker console. Handles command line
 * argument parsing to set up and run broker tasks.
 *
 * @version $Revision$
 */
public class Main {
    public static final int TASK_DEFAULT = 0;
    public static final int TASK_START   = 1;
    public static final int TASK_STOP    = 2;
    public static final int TASK_LIST    = 3;
    public static final int TASK_QUERY   = 4;

    public static final String TASK_DEFAULT_CLASS  = "org.apache.activemq.broker.console.DefaultTask";
    public static final String TASK_START_CLASS    = "org.apache.activemq.broker.console.StartTask";
    public static final String TASK_SHUTDOWN_CLASS = "org.apache.activemq.broker.console.ShutdownTask";
    public static final String TASK_LIST_CLASS     = "org.apache.activemq.broker.console.ListTask";
    public static final String TASK_QUERY_CLASS    = "org.apache.activemq.broker.console.QueryTask";

    private int           taskType;
    private File          activeMQHome;
    private ClassLoader   classLoader;
    private List          extensions = new ArrayList(5);

    public static void main(String[] args) {
        Main app = new Main();

        // Convert arguments to collection for easier management
        List tokens =  new LinkedList(Arrays.asList(args));

        // First token should be task type (start|stop|list|query)
        app.setTaskType(app.parseTask(tokens));

        // Parse for extension directory option
        app.parseExtensions(tokens);

        // Add default extension directories
        if(app.canUseExtdir()) {
            app.addExtensionDirectory(new File(app.getActiveMQHome(), "conf"));
            app.addExtensionDirectory(new File(app.getActiveMQHome(), "lib"));
            app.addExtensionDirectory(new File(new File(app.getActiveMQHome(), "lib"), "optional"));
        }

        // Succeeding tokens should be the task data
        try {
            switch (app.getTaskType()) {
                case TASK_START:   app.runTaskClass(TASK_START_CLASS, tokens);    break;
                case TASK_STOP:    app.runTaskClass(TASK_SHUTDOWN_CLASS, tokens); break;
                case TASK_LIST:    app.runTaskClass(TASK_LIST_CLASS, tokens);     break;
                case TASK_QUERY:   app.runTaskClass(TASK_QUERY_CLASS, tokens);    break;
                case TASK_DEFAULT: app.runTaskClass(TASK_DEFAULT_CLASS, tokens);  break;
                default:
                    System.out.println("Encountered unknown task type: " + app.getTaskType());
            }
        } catch (Throwable e) {
            System.out.println("Failed to execute main task. Reason: " + e);
        }
    }

    public int parseTask(List tokens) {
        if (tokens.isEmpty()) {
            // If no task, run the default task
            return TASK_DEFAULT;
        }

        // Process task token
        String taskToken = (String)tokens.remove(0);
        if (taskToken.equals("start")) {
            return TASK_START;
        } else if (taskToken.equals("stop")) {
            return TASK_STOP;
        } else if (taskToken.equals("list")) {
            return TASK_LIST;
        } else if (taskToken.equals("query")) {
            return TASK_QUERY;
        } else {
            // If not valid task, push back to list
            tokens.add(0, taskToken);
            return TASK_DEFAULT;
        }
    }

    public void parseExtensions(List tokens) {
        if (tokens.isEmpty()) {
            return;
        }
        
        String token = (String)tokens.get(0);

        // If token is an extension dir option
        if (token.equals("--extdir")) {
            // Process token
            tokens.remove(0);

            // If no extension directory is specified, or next token is another option
            if (tokens.isEmpty() || ((String)tokens.get(0)).startsWith("-")) {
                System.out.println("Extension directory not specified.");
                System.out.println("Ignoring extension directory option.");
                return;
            }

            // Process extension dir token
            File extDir = new File((String)tokens.remove(0));

            if(!canUseExtdir()) {
                System.out.println("Extension directory feature not available due to the system classpath being able to load: " + TASK_DEFAULT_CLASS);
                System.out.println("Ignoring extension directory option.");
                return;
            }

            if (!extDir.isDirectory()) {
                System.out.println("Extension directory specified is not valid directory: " + extDir);
                System.out.println("Ignoring extension directory option.");
                return;
            }

            addExtensionDirectory(extDir);
        }
    }

    public void runTaskClass(String taskClass, List tokens) throws Throwable {
        System.out.println("ACTIVEMQ_HOME: "+ getActiveMQHome());

        ClassLoader cl = getClassLoader();

        // Use reflection to run the task.
        try {
            Class task = cl.loadClass(taskClass);
            Method runTask = task.getMethod("runTask", new Class[] { List.class });
            runTask.invoke(task.newInstance(), new Object[] { tokens });
        } catch (InvocationTargetException e) {
            throw e.getCause();
        } catch (Throwable e) {
            throw e;
        }
    }

    public void addExtensionDirectory(File directory) {
        extensions.add(directory);
    }

    /**
     * The extension directory feature will not work if the broker factory is already in the classpath
     * since we have to load him from a child ClassLoader we build for it to work correctly.
     *
     * @return
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
        if(classLoader==null) {
            // Setup the ClassLoader
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

    public int getTaskType() {
        return taskType;
    }

    public void setTaskType(int taskType) {
        this.taskType = taskType;
    }

    public void setActiveMQHome(File activeMQHome) {
        this.activeMQHome = activeMQHome;
    }

    public File getActiveMQHome() {
        if(activeMQHome==null) {
            if(System.getProperty("activemq.home") != null) {
                activeMQHome = new File(System.getProperty("activemq.home"));
            }

            if(activeMQHome==null){
                // guess from the location of the jar
                URL url = Main.class.getClassLoader().getResource("org/apache/activemq/broker/Main.class");
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
}
