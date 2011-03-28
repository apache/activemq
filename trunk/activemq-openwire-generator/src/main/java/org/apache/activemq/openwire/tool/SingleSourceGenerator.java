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
package org.apache.activemq.openwire.tool;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.tools.ant.Project;
import org.apache.tools.ant.taskdefs.FixCRLF;
import org.codehaus.jam.JClass;
import org.codehaus.jam.JProperty;
import org.codehaus.jam.JamClassIterator;

/**
 * 
 */
public abstract class SingleSourceGenerator extends OpenWireGenerator {

    protected Set<String> manuallyMaintainedClasses = new HashSet<String>();
    protected File destFile;

    protected JClass jclass;
    protected JClass superclass;
    protected String simpleName;
    protected String className;
    protected String baseClass;
    protected List<JClass> sortedClasses;

    public SingleSourceGenerator() {
        initialiseManuallyMaintainedClasses();
    }

    public Object run() {

        if (destFile == null) {
            throw new IllegalArgumentException("No destFile defined!");
        }
        destFile.getParentFile().mkdirs();

        PrintWriter out = null;
        try {
            out = new PrintWriter(new FileWriter(destFile));

            ArrayList<JClass> classes = new ArrayList<JClass>();
            JamClassIterator iter = getClasses();
            while (iter.hasNext()) {
                jclass = iter.nextClass();
                if (isValidClass(jclass)) {
                    classes.add(jclass);
                }
            }
            sortedClasses = sort(classes);

            generateSetup(out);
            for (Iterator<JClass> iterator = sortedClasses.iterator(); iterator.hasNext();) {
                jclass = iterator.next();
                simpleName = jclass.getSimpleName();
                superclass = jclass.getSuperclass();
                className = getClassName(jclass);
                baseClass = getBaseClassName(jclass);

                System.out.println(getClass().getName() + " processing class: " + simpleName);
                generateFile(out);
            }
            generateTearDown(out);

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (out != null) {
                out.close();
            }
        }

        // Use the FixCRLF Ant Task to make sure the file has consistent
        // newlines
        // so that SVN does not complain on checkin.
        Project project = new Project();
        project.init();
        FixCRLF fixCRLF = new FixCRLF();
        fixCRLF.setProject(project);
        fixCRLF.setSrcdir(destFile.getParentFile());
        fixCRLF.setIncludes(destFile.getName());
        fixCRLF.execute();
        return null;
    }

    protected List<JClass> sort(List<JClass> classes) {
        return classes;
    }

    protected void generateTearDown(PrintWriter out) {
    }

    protected void generateSetup(PrintWriter out) {
    }

    /**
     * Returns all the valid properties available on the current class
     */
    public List<JProperty> getProperties() {
        List<JProperty> answer = new ArrayList<JProperty>();
        JProperty[] properties = jclass.getDeclaredProperties();
        for (int i = 0; i < properties.length; i++) {
            JProperty property = properties[i];
            if (isValidProperty(property)) {
                answer.add(property);
            }
        }
        return answer;
    }

    protected boolean isValidClass(JClass jclass) {
        if (jclass == null || jclass.getAnnotation("openwire:marshaller") == null) {
            return false;
        }
        return true;
        // return !manuallyMaintainedClasses.contains(jclass.getSimpleName());
    }

    protected abstract void generateFile(PrintWriter out) throws Exception;

    protected String getBaseClassName(JClass jclass) {
        String answer = "BaseDataStructure";
        if (superclass != null) {
            String name = superclass.getSimpleName();
            if (name != null && !name.equals("Object")) {
                answer = name;
            }
        }
        return answer;
    }

    protected String getClassName(JClass jclass) {
        return jclass.getSimpleName();
    }

    public boolean isAbstractClass() {
        return jclass != null && jclass.isAbstract();
    }

    public String getAbstractClassText() {
        return isAbstractClass() ? "abstract " : "";
    }

    public boolean isMarshallerAware() {
        return isMarshallAware(jclass);
    }

    protected void initialiseManuallyMaintainedClasses() {
        String[] names = {
            "ActiveMQDestination", "ActiveMQTempDestination", "ActiveMQQueue", "ActiveMQTopic", "ActiveMQTempQueue", "ActiveMQTempTopic", "BaseCommand", "ActiveMQMessage", "ActiveMQTextMessage",
            "ActiveMQMapMessage", "ActiveMQBytesMessage", "ActiveMQStreamMessage", "ActiveMQStreamMessage", "DataStructureSupport", "WireFormatInfo", "ActiveMQObjectMessage"
        };

        for (int i = 0; i < names.length; i++) {
            manuallyMaintainedClasses.add(names[i]);
        }
    }

    public String getBaseClass() {
        return baseClass;
    }

    public void setBaseClass(String baseClass) {
        this.baseClass = baseClass;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public File getDestFile() {
        return destFile;
    }

    public void setDestFile(File destFile) {
        this.destFile = destFile;
    }

    public JClass getJclass() {
        return jclass;
    }

    public void setJclass(JClass jclass) {
        this.jclass = jclass;
    }

    public Set<String> getManuallyMaintainedClasses() {
        return manuallyMaintainedClasses;
    }

    public void setManuallyMaintainedClasses(Set<String> manuallyMaintainedClasses) {
        this.manuallyMaintainedClasses = manuallyMaintainedClasses;
    }

    public String getSimpleName() {
        return simpleName;
    }

    public void setSimpleName(String simpleName) {
        this.simpleName = simpleName;
    }

    public JClass getSuperclass() {
        return superclass;
    }

    public void setSuperclass(JClass superclass) {
        this.superclass = superclass;
    }

}
