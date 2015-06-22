/*
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
package org.apache.activemq.console.command;

import org.w3c.dom.Attr;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

public class CreateCommand extends AbstractCommand {

    protected final String[] helpFile = new String[] {
        "Task Usage: Main create path/to/brokerA [create-options]",
        "Description:  Creates a runnable broker instance in the specified path.",
        "",
        "List Options:",
        "    --amqconf <file path>   Path to ActiveMQ conf file that will be used in the broker instance. Default is: conf/activemq.xml",
        "    --version               Display the version information.",
        "    -h,-?,--help            Display the create broker help information.",
        ""
    };

    protected final String DEFAULT_TARGET_ACTIVEMQ_CONF = "conf/activemq.xml"; // default activemq conf to create in the new broker instance
    protected final String DEFAULT_BROKERNAME_XPATH = "/beans/broker/@brokerName"; // default broker name xpath to change the broker name

    protected final String[] BASE_SUB_DIRS = { "bin", "conf" }; // default sub directories that will be created
    protected final String BROKER_NAME_REGEX = "[$][{]brokerName[}]"; // use to replace broker name property holders

    protected String amqConf = "conf/activemq.xml"; // default conf if no conf is specified via --amqconf

    // default files to create
    protected String[][] fileWriteMap = {
        { "winActivemq", "bin/${brokerName}.bat" },
        { "unixActivemq", "bin/${brokerName}" }
    };


    protected String brokerName;
    protected File amqHome;
    protected File targetAmqBase;

    @Override
    public String getName() {
        return "create";
    }

    @Override
    public String getOneLineDescription() {
        return "Creates a runnable broker instance in the specified path.";
    }

    protected void runTask(List<String> tokens) throws Exception {
        context.print("Running create broker task...");
        amqHome = new File(System.getProperty("activemq.home"));
        for (String token : tokens) {

            targetAmqBase = new File(token);
            brokerName = targetAmqBase.getName();


            if (targetAmqBase.exists()) {
                BufferedReader console = new BufferedReader(new InputStreamReader(System.in));
                String resp;
                while (true) {
                    context.print("Target directory (" + targetAmqBase.getCanonicalPath() + ") already exists. Overwrite (y/n): ");
                    resp = console.readLine();
                    if (resp.equalsIgnoreCase("y") || resp.equalsIgnoreCase("yes")) {
                        break;
                    } else if (resp.equalsIgnoreCase("n") || resp.equalsIgnoreCase("no")) {
                        return;
                    }
                }
            }

            context.print("Creating directory: " + targetAmqBase.getCanonicalPath());
            targetAmqBase.mkdirs();
            createSubDirs(targetAmqBase, BASE_SUB_DIRS);
            writeFileMapping(targetAmqBase, fileWriteMap);
            copyActivemqConf(amqHome, targetAmqBase, amqConf);
            copyConfDirectory(new File(amqHome, "conf"), new File(targetAmqBase, "conf"));
        }
    }

    /**
     * Handle the --amqconf options.
     *
     * @param token  - option token to handle
     * @param tokens - succeeding command arguments
     * @throws Exception
     */
    protected void handleOption(String token, List<String> tokens) throws Exception {
        if (token.startsWith("--amqconf")) {
            // If no amqconf specified, or next token is a new option
            if (tokens.isEmpty() || tokens.get(0).startsWith("-")) {
                context.printException(new IllegalArgumentException("Attributes to amqconf not specified"));
                return;
            }

            amqConf = tokens.remove(0);
        } else {
            // Let super class handle unknown option
            super.handleOption(token, tokens);
        }
    }

    protected void createSubDirs(File target, String[] subDirs) throws IOException {
        File subDirFile;
        for (String subDir : BASE_SUB_DIRS) {
            subDirFile = new File(target, subDir);
            context.print("Creating directory: " + subDirFile.getCanonicalPath());
            subDirFile.mkdirs();
        }
    }

    protected void writeFileMapping(File targetBase, String[][] fileWriteMapping) throws IOException {
        for (String[] fileWrite : fileWriteMapping) {
            File dest = new File(targetBase, resolveParam(BROKER_NAME_REGEX, brokerName, fileWrite[1]));
            context.print("Creating new file: " + dest.getCanonicalPath());
            writeFile(fileWrite[0], dest);
        }
    }

    protected void copyActivemqConf(File srcBase, File targetBase, String activemqConf) throws IOException, ParserConfigurationException, SAXException, TransformerException, XPathExpressionException {
        File src = new File(srcBase, activemqConf);

        if (!src.exists()) {
            throw new FileNotFoundException("File: " + src.getCanonicalPath() + " not found.");
        }

        File dest = new File(targetBase, DEFAULT_TARGET_ACTIVEMQ_CONF);
        context.print("Copying from: " + src.getCanonicalPath() + "\n          to: " + dest.getCanonicalPath());

        DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        Element docElem = builder.parse(src).getDocumentElement();

        XPath xpath = XPathFactory.newInstance().newXPath();
        Attr brokerNameAttr = (Attr) xpath.evaluate(DEFAULT_BROKERNAME_XPATH, docElem, XPathConstants.NODE);
        brokerNameAttr.setValue(brokerName);

        writeToFile(new DOMSource(docElem), dest);
    }

    protected void printHelp() {
        context.printHelp(helpFile);
    }

    // write the default files to create (i.e. script files)
    private void writeFile(String typeName, File dest) throws IOException {
        String data;
        if (typeName.equals("winActivemq")) {
            data = winActivemqData;
            data = resolveParam("[$][{]activemq.home[}]", amqHome.getCanonicalPath().replaceAll("[\\\\]", "/"), data);
            data = resolveParam("[$][{]activemq.base[}]", targetAmqBase.getCanonicalPath().replaceAll("[\\\\]", "/"), data);
        } else if (typeName.equals("unixActivemq")) {
            data = getUnixActivemqData();
            data = resolveParam("[$][{]activemq.home[}]", amqHome.getCanonicalPath().replaceAll("[\\\\]", "/"), data);
            data = resolveParam("[$][{]activemq.base[}]", targetAmqBase.getCanonicalPath().replaceAll("[\\\\]", "/"), data);
        } else {
            throw new IllegalStateException("Unknown file type: " + typeName);
        }

        ByteBuffer buf = ByteBuffer.allocate(data.length());
        buf.put(data.getBytes());
        buf.flip();

        try(FileOutputStream fos = new FileOutputStream(dest);
            FileChannel destinationChannel = fos.getChannel()) {
            destinationChannel.write(buf);
        }

        // Set file permissions available for Java 6.0 only
        dest.setExecutable(true);
        dest.setReadable(true);
        dest.setWritable(true);
    }

    // utlity method to write an xml source to file
    private void writeToFile(Source src, File file) throws TransformerException {
        TransformerFactory tFactory = TransformerFactory.newInstance();
        Transformer fileTransformer = tFactory.newTransformer();

        Result res = new StreamResult(file);
        fileTransformer.transform(src, res);
    }

    // utility method to copy one file to another
    private void copyFile(File from, File dest) throws IOException {
        if (!from.exists()) {
            return;
        }

        try(FileInputStream fis = new FileInputStream(from);
            FileChannel sourceChannel = fis.getChannel();
            FileOutputStream fos = new FileOutputStream(dest);
            FileChannel destinationChannel = fos.getChannel()) {
            sourceChannel.transferTo(0, sourceChannel.size(), destinationChannel);
        }
    }

    private void copyConfDirectory(File from, File dest) throws IOException {
        if (from.isDirectory()) {
            String files[] = from.list();

            for (String file : files) {
                File srcFile = new File(from, file);
                if (srcFile.isFile() && !srcFile.getName().equals("activemq.xml")) {
                    File destFile = new File(dest, file);
                    context.print("Copying from: " + srcFile.getCanonicalPath() + "\n          to: " + destFile.getCanonicalPath());
                    copyFile(srcFile, destFile);
                }
            }
        } else {
            throw new IOException(from + " is not a directory");
        }
    }

    // replace a property place holder (paramName) with the paramValue
    private String resolveParam(String paramName, String paramValue, String target) {
        return target.replaceAll(paramName, paramValue);
    }

    // Embedded windows script data
    private static final String winActivemqData =
        "@echo off\n"
            + "set ACTIVEMQ_HOME=\"${activemq.home}\"\n"
            + "set ACTIVEMQ_BASE=\"${activemq.base}\"\n"
            + "\n"
            + "set PARAM=%1\n"
            + ":getParam\n"
            + "shift\n"
            + "if \"%1\"==\"\" goto end\n"
            + "set PARAM=%PARAM% %1\n"
            + "goto getParam\n"
            + ":end\n"
            + "\n"
            + "%ACTIVEMQ_HOME%/bin/activemq %PARAM%";


   private String getUnixActivemqData() {
       StringBuffer res = new StringBuffer();
       res.append("## Figure out the ACTIVEMQ_BASE from the directory this script was run from\n");
       res.append("PRG=\"$0\"\n");
       res.append("progname=`basename \"$0\"`\n");
       res.append("saveddir=`pwd`\n");
       res.append("# need this for relative symlinks\n");
       res.append("dirname_prg=`dirname \"$PRG\"`\n");
       res.append("cd \"$dirname_prg\"\n");
       res.append("while [ -h \"$PRG\" ] ; do\n");
       res.append("  ls=`ls -ld \"$PRG\"`\n");
       res.append("  link=`expr \"$ls\" : '.*-> \\(.*\\)$'`\n");
       res.append("  if expr \"$link\" : '.*/.*' > /dev/null; then\n");
       res.append("    PRG=\"$link\"\n");
       res.append("  else\n");
       res.append("    PRG=`dirname \"$PRG\"`\"/$link\"\n");
       res.append("  fi\n");
       res.append("done\n");
       res.append("ACTIVEMQ_BASE=`dirname \"$PRG\"`/..\n");
       res.append("cd \"$saveddir\"\n\n");
       res.append("ACTIVEMQ_BASE=`cd \"$ACTIVEMQ_BASE\" && pwd`\n\n");
       res.append("## Enable remote debugging\n");
       res.append("#export ACTIVEMQ_DEBUG_OPTS=\"-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005\"\n\n");
       res.append("## Add system properties for this instance here (if needed), e.g\n");
       res.append("#export ACTIVEMQ_OPTS_MEMORY=\"-Xms256M -Xmx1G\"\n");
       res.append("#export ACTIVEMQ_OPTS=\"$ACTIVEMQ_OPTS_MEMORY -Dorg.apache.activemq.UseDedicatedTaskRunner=true -Djava.util.logging.config.file=logging.properties\"\n\n");
       res.append("export ACTIVEMQ_HOME=${activemq.home}\n");
       res.append("export ACTIVEMQ_BASE=$ACTIVEMQ_BASE\n\n");
       res.append("${ACTIVEMQ_HOME}/bin/activemq \"$@\"");
       return res.toString();
   }

}
