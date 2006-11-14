/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.LinkedList;
import java.util.List;

/**
 * Main class that can bootstrap an ActiveMQ broker console. Handles command line
 * argument parsing to set up and run broker tasks.
 *
 * @version $Revision$
 */
public class Main {

    public static final String TASK_DEFAULT_CLASS  = "org.apache.activemq.console.command.ShellCommand";

    private File          activeMQHome;
	private File          activeMQBase;
    private ClassLoader   classLoader;
    private List          classpaths = new ArrayList(5);
    private List          extensions = new ArrayList(5);


    private static boolean useDefExt = true;

    public static void main(String[] args) {
        Main app = new Main();

        // Convert arguments to collection for easier management
        List tokens =  new LinkedList(Arrays.asList(args));
        // Parse for extension directory option
        app.parseExtensions(tokens);

        // Add the following to the classpath:
        // 
        // ${activemq.base}/conf
        // ${activemq.base}/lib/* (only if activemq.base != activemq.home)
        // ${activemq.home}/lib/* 
        // ${activemq.base}/lib/optional/* (only if activemq.base != activemq.home)
        // ${activemq.home}/lib/optional/* 
        // 
        if(useDefExt && app.canUseExtdir()) {

        	boolean baseIsHome = app.getActiveMQBase().equals(app.getActiveMQHome()); 
        	
            app.addClassPath(new File(app.getActiveMQBase(), "conf"));
            
            if(!baseIsHome) {
                app.addExtensionDirectory(new File(app.getActiveMQBase(), "lib"));
            }
            app.addExtensionDirectory(new File(app.getActiveMQHome(), "lib"));
            
            if(!baseIsHome) {
                app.addExtensionDirectory(new File(new File(app.getActiveMQBase(), "lib"), "optional"));
            }
            app.addExtensionDirectory(new File(new File(app.getActiveMQHome(), "lib"), "optional"));
            
        }

        try {
            app.runTaskClass(tokens);
        } catch (ClassNotFoundException e) {
            System.out.println("Could not load class: " + e.getMessage());
            try {
				ClassLoader cl = app.getClassLoader();
				if( cl!=null ) {
		            System.out.println("Class loader setup: ");
					printClassLoaderTree(cl);
				}
			} catch (MalformedURLException e1) {
			}
        } catch (Throwable e) {
            System.out.println("Failed to execute main task. Reason: " + e);
        }
    }

    /**
     * Print out what's in the classloader tree being used. 
     * 
     * @param cl
     * @return
     */
	private static int printClassLoaderTree(ClassLoader cl) {
		int depth = 0;
		if( cl.getParent()!=null ) {
			depth = printClassLoaderTree(cl.getParent())+1;
		}
		
		StringBuffer indent = new StringBuffer();
		for (int i = 0; i < depth; i++) {
			indent.append("  ");
		}
		
		if( cl instanceof URLClassLoader ) {
			URLClassLoader ucl = (URLClassLoader) cl;
			System.out.println(indent+cl.getClass().getName()+" {");
			URL[] urls = ucl.getURLs();
			for (int i = 0; i < urls.length; i++) {
				System.out.println(indent+"  "+urls[i]);
			}
			System.out.println(indent+"}");
		} else {
			System.out.println(indent+cl.getClass().getName());
		}
		return depth;
	}

	public void parseExtensions(List tokens) {
        if (tokens.isEmpty()) {
            return;
        }

        int count = tokens.size();
        int i = 0;

        // Parse for all --extdir and --noDefExt options
        while (i < count) {
            String token = (String)tokens.get(i);
            // If token is an extension dir option
            if (token.equals("--extdir")) {
                // Process token
                count--;
                tokens.remove(i);

                // If no extension directory is specified, or next token is another option
                if (i >= count || ((String)tokens.get(i)).startsWith("-")) {
                    System.out.println("Extension directory not specified.");
                    System.out.println("Ignoring extension directory option.");
                    continue;
                }

                // Process extension dir token
                count--;
                File extDir = new File((String)tokens.remove(i));

                if(!canUseExtdir()) {
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
            } else if (token.equals("--noDefExt")) { // If token is --noDefExt option
                count--;
                tokens.remove(i);
                useDefExt = false;
            } else {
                i++;
            }
		}

    }

    public void runTaskClass(List tokens) throws Throwable {
    	
        System.out.println("ACTIVEMQ_HOME: "+ getActiveMQHome());
        System.out.println("ACTIVEMQ_BASE: "+ getActiveMQBase());

        ClassLoader cl = getClassLoader();

        // Use reflection to run the task.
        try {
            String[] args = (String[]) tokens.toArray(new String[tokens.size()]);
            Class task = cl.loadClass(TASK_DEFAULT_CLASS);
            Method runTask = task.getMethod("main", new Class[] { String[].class, InputStream.class, PrintStream.class });
            runTask.invoke(task.newInstance(), new Object[] { args, System.in, System.out });
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    public void addExtensionDirectory(File directory) {
        extensions.add(directory);
    }
    
    private void addClassPath(File file) {
        classpaths.add(file);
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
            if (!extensions.isEmpty() || !classpaths.isEmpty()) {

                ArrayList urls = new ArrayList();
                
                for (Iterator iter = classpaths.iterator(); iter.hasNext();) {
                    File dir = (File) iter.next();
                    // try{ System.out.println("Adding to classpath: " + dir.getCanonicalPath()); }catch(Exception e){}
                    urls.add(dir.toURL());
                }
                
                for (Iterator iter = extensions.iterator(); iter.hasNext();) {
                    File dir = (File) iter.next();
                    if( dir.isDirectory() ) {
	                    File[] files = dir.listFiles();
	                    if( files!=null ) {
	                    	
	                    	// Sort the jars so that classpath built is consistently
	                    	// in the same order.  Also allows us to use jar names to control
	                    	// classpath order.
	                    	Arrays.sort(files, new Comparator(){
								public int compare(Object o1, Object o2) {
									File f1 = (File) o1;
									File f2 = (File) o2;
									return f1.getName().compareTo(f2.getName());
								}
							});
	                    	
	                        for (int j = 0; j < files.length; j++) {
	                            if( files[j].getName().endsWith(".zip") || files[j].getName().endsWith(".jar") ) {
	                                // try{ System.out.println("Adding to classpath: " + files[j].getCanonicalPath()); }catch(Exception e){}
	                                urls.add(files[j].toURL());
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
        if(activeMQHome==null) {
            if(System.getProperty("activemq.home") != null) {
                activeMQHome = new File(System.getProperty("activemq.home"));
            }

            if(activeMQHome==null){
                // guess from the location of the jar
                URL url = Main.class.getClassLoader().getResource("org/apache/activemq/console/Main.class");
                if (url != null) {
                    try {
                        JarURLConnection jarConnection = (JarURLConnection) url.openConnection();
                        url = jarConnection.getJarFileURL();
                        URI baseURI = new URI(url.toString()).resolve("..");
                        activeMQHome = new File(baseURI).getCanonicalFile();
                        System.setProperty("activemq.home",activeMQHome.getAbsolutePath());
                    } catch (Exception ignored) {
                    }
                }
            }

            if(activeMQHome==null){
                activeMQHome = new File("../.");
                System.setProperty("activemq.home",activeMQHome.getAbsolutePath());
            }
        }
        
        return activeMQHome;
    }
    
    public File getActiveMQBase() {
        if(activeMQBase==null) {
            if(System.getProperty("activemq.base") != null) {
            	activeMQBase = new File(System.getProperty("activemq.base"));
            }
            
            if(activeMQBase==null){
                activeMQBase = getActiveMQHome();
                System.setProperty("activemq.base",activeMQBase.getAbsolutePath());
            }
        }
        
        return activeMQBase;
    }

}
