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
package org.apache.activemq.maven;

import groovy.lang.GroovyShell;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.codehaus.groovy.control.CompilationFailedException;
import org.codehaus.jam.JamService;
import org.codehaus.jam.JamServiceFactory;
import org.codehaus.jam.JamServiceParams;
import org.codehaus.plexus.archiver.manager.ArchiverManager;

/**
 * Greates a Mojo for Gram.
 * 
 * This module is largly based on the Gram class here:
 * http://cvs.groovy.codehaus.org/browse/groovy/groovy/modules/gram/src/main/org/codehaus/gram/Gram.java?r=1.4
 * 
 * We need to get this moved over the groovy project eventually.. Putting in ActiveMQ for now so we can keep going.
 * 
 * @goal gram
 * @description Runs the Gram code generator
 */
public class GramMojo extends AbstractMojo 
{
    /**
     * Source directories of the project.
     *
     * @parameter expression="${project.compileSourceRoots}"
     * @required
     * @readonly
     */
    private List sourceDirs;


    /**
     * @parameter
     * @required
     */
    String scripts = "";
    
    /**
     * @parameter
     */
    Map groovyProperties = Collections.EMPTY_MAP;
    
    /**
     * To look up Archiver/UnArchiver implementations
     *
     * @parameter expression="${component.org.codehaus.plexus.archiver.manager.ArchiverManager}"
     * @required
     */
    protected ArchiverManager archiverManager;
    
    public void execute() throws MojoExecutionException 
    {

        try {
        	
            System.out.println("Parsing source files in: " + sourceDirs);

            JamServiceFactory jamServiceFactory = JamServiceFactory.getInstance();
            JamServiceParams params = jamServiceFactory.createServiceParams();
            
            File[] dirs = new File[sourceDirs.size()];
            {
	            int i=0;
	            for (Iterator iter = sourceDirs.iterator(); iter.hasNext();) {
					dirs[i++] = new File((String) iter.next());
				}
            }
            
            params.includeSourcePattern(dirs, "**/*.java");
            JamService jam = jamServiceFactory.createService(params);

            String[] scriptsArray = scripts.split(":");
            for (int i = 1; i < scriptsArray.length; i++) {
                String script = scriptsArray[i].trim();
                if(script.length() > 0 ) {
	                getLog().info("Evaluating Groovy script: " + script);
	                execute(jam, script);
                }
            }
        }
        catch (Exception e) {
        	getLog().error("Caught: " + e, e);
        }
    }
    
    public void execute(JamService jam, String script) throws CompilationFailedException, IOException {
        File file = new File(script);
        if (file.isFile()) {
            GroovyShell shell = createShell(jam);
            shell.evaluate(file);
        }
        else {
            // lets try load the script on the classpath
            InputStream in = getClass().getClassLoader().getResourceAsStream(script);
            if (in == null) {
                in = Thread.currentThread().getContextClassLoader().getResourceAsStream(script);
                if (in == null) {
                    throw new IOException("No script called: " + script + " could be found on the classpath or the file system");
                }
            }
            GroovyShell shell = createShell(jam);
            shell.evaluate(in, script);
        }
    }

    protected GroovyShell createShell(JamService jam) {
        GroovyShell answer = new GroovyShell();
        for (Iterator iter = groovyProperties.entrySet().iterator(); iter.hasNext();) {
			Map.Entry entry = (Map.Entry) iter.next();
	        answer.setProperty((String) entry.getKey(), entry.getValue());
		}
        answer.setProperty("jam", jam);
        answer.setProperty("classes", jam.getAllClasses());
        return answer;
    }
    
}
