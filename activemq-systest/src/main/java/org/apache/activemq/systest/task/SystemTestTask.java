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
package org.apache.activemq.systest.task;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.DirectoryScanner;
import org.apache.tools.ant.taskdefs.MatchingTask;
import org.apache.tools.ant.types.FileSet;
import org.apache.tools.ant.types.Path;
import org.apache.tools.ant.types.Reference;
import org.codehaus.jam.JClass;
import org.codehaus.jam.JamService;
import org.codehaus.jam.JamServiceFactory;
import org.codehaus.jam.JamServiceParams;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;

/**
 * An Ant task for executing Gram scripts, which are Groovy scripts executed on
 * the JAM context.
 * 
 * @version $Revision: 1.2 $
 */
public class SystemTestTask extends MatchingTask {

    private static final String SCENARIOS_PROPERTIES_FILE = "activemq-scenarios.properties";

    private Path srcDir = null;
    private Path mToolpath = null;
    private Path mClasspath = null;
    private String mIncludes = "**/*.java";
    private File destDir;
    private FileSet clientFiles;
    private FileSet brokerFiles;
    private File scenariosFile;

    public File getScenariosFile() {
        return scenariosFile;
    }

    public void setScenariosFile(File scenariosFile) {
        this.scenariosFile = scenariosFile;
    }

    /**
     * Sets the directory into which source files should be generated.
     */
    public void setDestDir(File destDir) {
        this.destDir = destDir;
    }

    public void setSrcDir(Path srcDir) {
        this.srcDir = srcDir;
    }

    public void setToolpath(Path path) {
        if (mToolpath == null) {
            mToolpath = path;
        }
        else {
            mToolpath.append(path);
        }
    }

    public void setToolpathRef(Reference r) {
        createToolpath().setRefid(r);
    }

    public FileSet createBrokerFiles() {
        return new FileSet();
    }

    public FileSet getBrokerFiles() {
        return brokerFiles;
    }

    public void setBrokerFiles(FileSet brokerFiles) {
        this.brokerFiles = brokerFiles;
    }

    public FileSet createClientFiles() {
        return new FileSet();
    }

    public FileSet getClientFiles() {
        return clientFiles;
    }

    public void setClientFiles(FileSet clientFiles) {
        this.clientFiles = clientFiles;
    }

    public Path createToolpath() {
        if (mToolpath == null) {
            mToolpath = new Path(getProject());
        }
        return mToolpath.createPath();
    }

    public void setClasspath(Path path) {
        if (mClasspath == null) {
            mClasspath = path;
        }
        else {
            mClasspath.append(path);
        }
    }

    public void setClasspathRef(Reference r) {
        createClasspath().setRefid(r);
    }

    public Path createClasspath() {
        if (mClasspath == null) {
            mClasspath = new Path(getProject());
        }
        return mClasspath.createPath();
    }

    public void execute() throws BuildException {
        /*
         * if (srcDir == null) { throw new BuildException("'srcDir' must be
         * specified"); }
         */
        if (scenariosFile == null) {
            throw new BuildException("'scenariosFile' must be specified");
        }
        if (destDir == null) {
            throw new BuildException("'destDir' must be specified");
        }
        if (clientFiles == null) {
            throw new BuildException("'clientFiles' must be specified");
        }
        if (brokerFiles == null) {
            throw new BuildException("'clientFiles' must be specified");
        }
        JamServiceFactory jamServiceFactory = JamServiceFactory.getInstance();
        JamServiceParams serviceParams = jamServiceFactory.createServiceParams();
        if (mToolpath != null) {
            File[] tcp = path2files(mToolpath);
            for (int i = 0; i < tcp.length; i++) {
                serviceParams.addToolClasspath(tcp[i]);
            }
        }
        if (mClasspath != null) {
            File[] cp = path2files(mClasspath);
            for (int i = 0; i < cp.length; i++) {
                serviceParams.addClasspath(cp[i]);
            }
        }

        JClass[] classes = null;
        File propertiesFile = scenariosFile;
        try {
            if (srcDir != null) {
                serviceParams.includeSourcePattern(path2files(srcDir), mIncludes);
                JamService jam = jamServiceFactory.createService(serviceParams);
                classes = jam.getAllClasses();
            }
            else {
                // lets try load the properties file
                classes = loadScenarioClasses();
                propertiesFile = null;
            }
            DirectoryScanner clientsScanner = clientFiles.getDirectoryScanner(getProject());
            DirectoryScanner brokersScanner = brokerFiles.getDirectoryScanner(getProject());
            SystemTestGenerator generator = new SystemTestGenerator(classes, destDir, clientsScanner, brokersScanner, getProject().getBaseDir(), propertiesFile);
            generator.generate();

            log("...done.");
        }
        catch (Exception e) {
            throw new BuildException(e);
        }
    }

    protected JClass[] loadScenarioClasses() throws IOException {
        InputStream in = getClass().getClassLoader().getResourceAsStream(SCENARIOS_PROPERTIES_FILE);
        if (in == null) {
            ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
            if (contextClassLoader != null) {
                in = contextClassLoader.getResourceAsStream(SCENARIOS_PROPERTIES_FILE);
            }
            if (in == null) {
                throw new IOException("Could not find ActiveMQ scenarios properties file on the classpath: " + SCENARIOS_PROPERTIES_FILE);
            }
        }
        Properties properties = new Properties();
        properties.load(in);
        List list = new ArrayList();
        for (Iterator iter = properties.entrySet().iterator(); iter.hasNext();) {
            Map.Entry entry = (Map.Entry) iter.next();
            String className = (String) entry.getKey();
            String names = (String) entry.getValue();
            String[] interfaceNameArray = parseNames(names);
            list.add(new ScenarioJClassStub(className, interfaceNameArray));
        }
        JClass[] answer = new JClass[list.size()];
        list.toArray(answer);
        return answer;
    }

    protected String[] parseNames(String names) {
        StringTokenizer iter = new StringTokenizer(names);
        List list = new ArrayList();
        while (iter.hasMoreTokens()) {
            String text = iter.nextToken().trim();
            if (text.length() > 0) {
                list.add(text);
            }
        }
        String[] answer = new String[list.size()];
        list.toArray(answer);
        return answer;
    }

    protected File[] path2files(Path path) {
        String[] list = path.list();
        File[] out = new File[list.length];
        for (int i = 0; i < out.length; i++) {
            out[i] = new File(list[i]).getAbsoluteFile();
        }
        return out;
    }
}
