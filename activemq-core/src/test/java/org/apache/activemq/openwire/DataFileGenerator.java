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
package org.apache.activemq.openwire;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;

import junit.framework.Assert;

abstract public class DataFileGenerator extends Assert {

    static final File moduleBaseDir;
    static final File controlDir;
    static final File classFileDir;

    static {
        moduleBaseDir = new File(System.getProperty("basedir", "."));
        controlDir = new File(moduleBaseDir, "src/test/resources/openwire-control");
        classFileDir = new File(moduleBaseDir, "src/test/java/org/apache/activemq/openwire");
    }

    public static void main(String[] args) throws Exception {
        generateControlFiles();
    }

    /**
     * @param srcdir
     * @return
     * @throws ClassNotFoundException
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    public static ArrayList getAllDataFileGenerators() throws Exception {
        // System.out.println("Looking for generators in : "+classFileDir);
        ArrayList l = new ArrayList();
        File[] files = classFileDir.listFiles();
        for (int i = 0; files != null && i < files.length; i++) {
            File file = files[i];
            if (file.getName().endsWith("Data.java")) {
                String cn = file.getName();
                cn = cn.substring(0, cn.length() - ".java".length());
                Class clazz = DataFileGenerator.class.getClassLoader().loadClass("org.apache.activemq.openwire." + cn);
                l.add((DataFileGenerator)clazz.newInstance());
            }
        }
        return l;
    }

    private static void generateControlFiles() throws Exception {
        ArrayList generators = getAllDataFileGenerators();
        for (Iterator iter = generators.iterator(); iter.hasNext();) {
            DataFileGenerator object = (DataFileGenerator)iter.next();
            try {
                // System.out.println("Processing: "+object.getClass());
                object.generateControlFile();
            } catch (Exception e) {
                // System.err.println("Error while processing:
                // "+object.getClass() + ". Reason: " + e);
            }
        }
    }

    public void generateControlFile() throws Exception {
        controlDir.mkdirs();
        File dataFile = new File(controlDir, getClass().getName() + ".bin");

        OpenWireFormat wf = new OpenWireFormat();
        wf.setCacheEnabled(false);
        wf.setStackTraceEnabled(false);
        wf.setVersion(1);

        FileOutputStream os = new FileOutputStream(dataFile);
        DataOutputStream ds = new DataOutputStream(os);
        wf.marshal(createObject(), ds);
        ds.close();
    }

    public InputStream generateInputStream() throws Exception {
        OpenWireFormat wf = new OpenWireFormat();
        wf.setCacheEnabled(false);
        wf.setStackTraceEnabled(false);
        wf.setVersion(1);

        ByteArrayOutputStream os = new ByteArrayOutputStream();
        DataOutputStream ds = new DataOutputStream(os);
        wf.marshal(createObject(), ds);
        ds.close();

        return new ByteArrayInputStream(os.toByteArray());
    }

    public static void assertAllControlFileAreEqual() throws Exception {
        ArrayList generators = getAllDataFileGenerators();
        for (Iterator iter = generators.iterator(); iter.hasNext();) {
            DataFileGenerator object = (DataFileGenerator)iter.next();
            // System.out.println("Processing: "+object.getClass());
            object.assertControlFileIsEqual();
        }
    }

    public void assertControlFileIsEqual() throws Exception {
        File dataFile = new File(controlDir, getClass().getName() + ".bin");
        FileInputStream is1 = new FileInputStream(dataFile);
        int pos = 0;
        try {
            InputStream is2 = generateInputStream();
            int a = is1.read();
            int b = is2.read();
            pos++;
            assertEquals("Data does not match control file: " + dataFile + " at byte position " + pos, a, b);
            while (a >= 0 && b >= 0) {
                a = is1.read();
                b = is2.read();
                pos++;
                assertEquals(a, b);
            }
            is2.close();
        } finally {
            is1.close();
        }
    }

    abstract protected Object createObject() throws IOException;
}
