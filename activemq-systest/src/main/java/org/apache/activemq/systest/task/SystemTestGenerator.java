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

import org.apache.activemq.systest.DestinationFactory;
import org.apache.activemq.systest.QueueOnlyScenario;
import org.apache.activemq.systest.Scenario;
import org.apache.activemq.systest.ScenarioTestSuite;
import org.apache.activemq.systest.TopicOnlyScenario;
import org.apache.tools.ant.DirectoryScanner;
import org.codehaus.jam.JClass;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 
 * @version $Revision: 1.1 $
 */
public class SystemTestGenerator {

    private JClass[] classes;
    private final File destDir;
    private final DirectoryScanner clientsScanner;
    private final DirectoryScanner brokersScanner;
    private final File baseDir;
    private final File scenariosFile;
    private String licenseHeader;
    
    public SystemTestGenerator(JClass[] classes, File destDir, DirectoryScanner clientsScanner, DirectoryScanner brokersScanner, File baseDir,
            File scenariosFile) {
        this.classes = classes;
        this.destDir = destDir;
        this.clientsScanner = clientsScanner;
        this.brokersScanner = brokersScanner;
        this.baseDir = baseDir;
        this.scenariosFile = scenariosFile;
    }

    public void generate() throws IOException {
        List list = new ArrayList();
        for (int i = 0; i < classes.length; i++) {
            JClass type = classes[i];
            if (implementsInterface(type, Scenario.class) && !type.isAbstract() && !type.isInterface()) {
                generateTestsFor(type);
                list.add(type);
            }
        }

        // now lets generate a list of all the available
        if (scenariosFile != null) {
            generatePropertiesFile(list);
        }
    }

    protected void generatePropertiesFile(List list) throws IOException {
        PrintWriter writer = new PrintWriter(new FileWriter(scenariosFile));
        try {
            for (Iterator iter = list.iterator(); iter.hasNext();) {
                JClass type = (JClass) iter.next();
                writer.print(type.getQualifiedName());
                writer.print(" = ");
                writeInterfaces(writer, type);
                writer.println();
            }
        }
        finally {
            writer.close();
        }
    }

    protected void writeInterfaces(PrintWriter writer, JClass type) {
        List interfaces = new ArrayList();
        addAllInterfaces(interfaces, type);
        boolean first = true;
        for (Iterator iter = interfaces.iterator(); iter.hasNext();) {
            JClass interfaceType = (JClass) iter.next();
            if (first) {
                first = false;
            }
            else {
                writer.print(", ");
            }
            writer.print(interfaceType.getQualifiedName());
        }
    }

    protected void addAllInterfaces(List list, JClass type) {
        JClass[] interfaces = type.getInterfaces();
        for (int i = 0; i < interfaces.length; i++) {
            JClass interfaceType = interfaces[i];
            list.add(interfaceType);
        }
        JClass superclass = type.getSuperclass();
        if (superclass != null) {
            addAllInterfaces(list, superclass);
        }
    }

    protected void generateTestsFor(JClass type) throws IOException {
        String[] files = clientsScanner.getIncludedFiles();
        for (int i = 0; i < files.length; i++) {
            String name = files[i];
            File file = new File(clientsScanner.getBasedir(), name);
            generateTestsFor(type, name, file);
        }
    }

    protected void generateTestsFor(JClass type, String clientName, File clientFile) throws IOException {
        String[] files = brokersScanner.getIncludedFiles();
        for (int i = 0; i < files.length; i++) {
            String name = files[i];
            File basedir = brokersScanner.getBasedir();
            File file = new File(basedir, name);

            if (!implementsInterface(type, TopicOnlyScenario.class)) {
                generateTestsFor(type, clientName, clientFile, name, file, DestinationFactory.QUEUE);
            }
            if (!implementsInterface(type, QueueOnlyScenario.class)) {
                generateTestsFor(type, clientName, clientFile, name, file, DestinationFactory.TOPIC);
            }
        }
    }

    protected void generateTestsFor(JClass type, String clientName, File clientFile, String brokerName, File brokerFile, int destinationType)
            throws IOException {
        String clientPrefix = trimPostFix(clientName);
        String brokerPrefix = trimPostFix(brokerName);

        String destinationName = ScenarioTestSuite.destinationDescription(destinationType);
        String[] paths = { "org", "activemq", "systest", brokerPrefix, destinationName, clientPrefix };
        String packageName = asPackageName(paths);

        File dir = makeDirectories(paths);
        File file = new File(dir, type.getSimpleName() + "Test.java");
        PrintWriter writer = new PrintWriter(new FileWriter(file));
        try {
            System.out.println("Generating: " + file);
            generateFile(writer, type, clientFile, brokerFile, packageName, destinationType);
        }
        finally {
            writer.close();
        }
    }

    protected void generateFile(PrintWriter writer, JClass type, File clientFile, File brokerFile, String packageName, int destinationType) throws IOException {
        writer.println(getLicenseHeader());
        writer.println("package " + packageName + ";");
        writer.println();
        writer.println("import org.apache.activemq.systest.DestinationFactory;");
        writer.println("import org.apache.activemq.systest.ScenarioTestSuite;");
        writer.println("import " + type.getQualifiedName() + ";");
        writer.println("import org.springframework.context.ApplicationContext;");
        writer.println("import org.springframework.context.support.FileSystemXmlApplicationContext;");
        writer.println();
        writer.println("import junit.framework.TestSuite;");
        writer.println();
        writer.println("/**");
        writer.println(" * System test case for Scenario: " + type.getSimpleName());
        writer.println(" *");
        writer.println(" *");
        writer.println(" * NOTE!: This file is auto generated - do not modify!");
        writer.println(" *        if you need to make a change, please see SystemTestGenerator code");
        writer.println(" *        in the activemq-systest module in ActiveMQ 4.x");
        writer.println(" *");
        writer.println(" * @version $Revision:$");
        writer.println(" */");
        writer.println("public class " + type.getSimpleName() + "Test extends ScenarioTestSuite {");
        writer.println();
        writer.println("    public static TestSuite suite() throws Exception {");
        writer.println("        ApplicationContext clientContext = new FileSystemXmlApplicationContext(\"" + relativePath(clientFile) + "\");");
        writer.println("        ApplicationContext brokerContext = new FileSystemXmlApplicationContext(\"" + relativePath(brokerFile) + "\");");
        writer.println("        Class[] scenarios = { " + type.getSimpleName() + ".class };");
        writer.println("        return createSuite(clientContext, brokerContext, scenarios, " + destinationExpression(destinationType) + ");");
        writer.println("    }");
        writer.println("}");
    }

    protected String destinationExpression(int destinationType) {
        switch (destinationType) {
        case DestinationFactory.QUEUE:
            return "DestinationFactory.QUEUE";

        default:
            return "DestinationFactory.TOPIC";
        }
    }

    protected String relativePath(File file) {
        String name = file.toString();
        String prefix = baseDir.toString();
        if (name.startsWith(prefix)) {
            return name.substring(prefix.length() + 1);
        }
        return name;
    }

    protected File makeDirectories(String[] paths) {
        File dir = destDir;
        for (int i = 0; i < paths.length; i++) {
            dir = new File(dir, paths[i]);
        }
        dir.mkdirs();
        return dir;
    }

    protected String asPackageName(String[] paths) {
        StringBuffer buffer = new StringBuffer(paths[0]);
        for (int i = 1; i < paths.length; i++) {
            buffer.append(".");
            buffer.append(paths[i]);
        }
        return buffer.toString();
    }

    protected String trimPostFix(String uri) {
        int idx = uri.lastIndexOf('.');
        if (idx > 0) {
            return uri.substring(0, idx);
        }
        return uri;
    }

    protected boolean implementsInterface(JClass type, Class interfaceClass) {
        JClass[] interfaces = type.getInterfaces();
        for (int i = 0; i < interfaces.length; i++) {
            JClass anInterface = interfaces[i];
            if (anInterface.getQualifiedName().equals(interfaceClass.getName())) {
                return true;
            }
        }
        JClass superclass = type.getSuperclass();
        if (superclass == null || superclass == type) {
            return false;
        }
        else {
            return implementsInterface(superclass, interfaceClass);
        }
    }

    public String getLicenseHeader() throws IOException {
        if( licenseHeader == null ) {
            // read the LICENSE_HEADER.txt into the licenseHeader variable.
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            InputStream is = SystemTestGenerator.class.getResourceAsStream("LICENSE_HEADER.txt");
            try {
                int c;
                while( (c=is.read())>=0 ) {
                    baos.write(c);
                }
            } finally {
                is.close();
            }
            baos.close();
            licenseHeader = new String(baos.toByteArray(),"UTF-8");
        }
        return licenseHeader;
    }

    public void setLicenseHeader(String licenseHeader) {
        this.licenseHeader = licenseHeader;
    }

}
