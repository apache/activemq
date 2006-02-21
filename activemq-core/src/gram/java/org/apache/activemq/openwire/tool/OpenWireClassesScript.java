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

import mx4j.tools.adaptor.http.GetAttributeCommandProcessor;

import org.codehaus.jam.JClass;
import org.codehaus.jam.JamClassIterator;

import java.io.File;
import java.io.*;
import java.util.*;

/**
 * 
 * @version $Revision$
 */
public abstract class OpenWireClassesScript extends OpenWireScript {
    protected Set manuallyMaintainedClasses = new HashSet();
    protected File destDir = new File("target/generated/classes");
    protected File destFile;
    protected String filePostFix = "";

    protected JClass jclass;
    protected JClass superclass;
    protected String simpleName;
    protected String className;
    protected String baseClass;
    protected StringBuffer buffer;

    public OpenWireClassesScript() {
        String[] names = { "ActiveMQDestination", "ActiveMQTempDestination", "ActiveMQQueue", "ActiveMQTopic", "ActiveMQTempQueue", "ActiveMQTempTopic",
                "BaseCommand", "ActiveMQMessage", "ActiveMQTextMessage", "ActiveMQMapMessage", "ActiveMQBytesMessage", "ActiveMQStreamMessage",
                "ActiveMQStreamMessage", "DataStructureSupport" };

        for (int i = 0; i < names.length; i++) {
            manuallyMaintainedClasses.add(names[i]);
        }
    }

    public Object run() {
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

    protected boolean isValidClass(JClass jclass) {
        if (jclass.getAnnotation("openwire:marshaller") == null) {
            return false;
        }
        return manuallyMaintainedClasses.contains(jclass.getSimpleName());
    }

    protected void processClass(JClass jclass) {
        simpleName = jclass.getSimpleName();
        superclass = jclass.getSuperclass();

        System.out.println("Processing class: " + simpleName);

        className = getClassName(jclass);

        destFile = new File(destDir, className + filePostFix);

        baseClass = getBaseClassName(jclass);

        PrintWriter out = null;
        try {
            out = new PrintWriter(new FileWriter(destFile));
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
        return "AbstractCommand";
    }

}
