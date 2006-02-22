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
package org.apache.activemq.openwire.tool;

import org.codehaus.jam.JClass;
import org.codehaus.jam.JProperty;
import org.codehaus.jam.JamClassIterator;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.*;

/**
 * 
 * @version $Revision$
 */
public abstract class OpenWireClassesScript extends OpenWireScript {
    protected Set manuallyMaintainedClasses = new HashSet();
    protected File destDir;
    protected File destFile;

    protected JClass jclass;
    protected JClass superclass;
    protected String simpleName;
    protected String className;
    protected String baseClass;
    protected StringBuffer buffer;

    public OpenWireClassesScript() {
        initialiseManuallyMaintainedClasses();
    }

    public Object run() {
        if (destDir == null) {
            throw new IllegalArgumentException("No destDir defined!");
        }
        System.out.println(getClass().getName() + " generating files in: " + destDir);
        destDir.mkdirs();
        buffer = new StringBuffer();

        JamClassIterator iter = getClasses();
        while (iter.hasNext()) {
            jclass = iter.nextClass();
            if (isValidClass(jclass)) {
                processClass(jclass);
            }
        }
        return null;
    }

    /**
     * Returns all the valid properties available on the current class
     */
    public List getProperties() {
        List answer = new ArrayList();
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
        if (jclass.getAnnotation("openwire:marshaller") == null) {
            return false;
        }
        return !manuallyMaintainedClasses.contains(jclass.getSimpleName());
    }

    protected void processClass(JClass jclass) {
        simpleName = jclass.getSimpleName();
        superclass = jclass.getSuperclass();

        System.out.println(getClass().getName() + " processing class: " + simpleName);

        className = getClassName(jclass);

        destFile = new File(destDir, className + filePostFix);

        baseClass = getBaseClassName(jclass);

        PrintWriter out = null;
        try {
            out = new PrintWriter(new FileWriter(destFile));
            generateFile(out);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        finally {
            if (out != null) {
                out.close();
            }
        }
    }

    protected abstract void generateFile(PrintWriter out);

    protected String getBaseClassName(JClass jclass) {
        String answer = "AbstractCommand";
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
        return jclass != null & jclass.isAbstract();
    }

    public String getAbstractClassText() {
        return isAbstractClass() ? "abstract " : "";
    }
    
    public boolean isMarshallerAware() {
        return isMarshallAware(jclass);
    }

    protected void initialiseManuallyMaintainedClasses() {
        String[] names = { "ActiveMQDestination", "ActiveMQTempDestination", "ActiveMQQueue", "ActiveMQTopic", "ActiveMQTempQueue", "ActiveMQTempTopic",
                "BaseCommand", "ActiveMQMessage", "ActiveMQTextMessage", "ActiveMQMapMessage", "ActiveMQBytesMessage", "ActiveMQStreamMessage",
                "ActiveMQStreamMessage", "DataStructureSupport" };

        for (int i = 0; i < names.length; i++) {
            manuallyMaintainedClasses.add(names[i]);
        }
    }

}
