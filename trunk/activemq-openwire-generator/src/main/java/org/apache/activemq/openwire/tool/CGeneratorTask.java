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

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.Task;
import org.codehaus.jam.JamService;
import org.codehaus.jam.JamServiceFactory;
import org.codehaus.jam.JamServiceParams;

/**
 * 
 */
public class CGeneratorTask extends Task {

    int version = 2;
    File source = new File(".");
    File target = new File(".");

    public static void main(String[] args) {

        Project project = new Project();
        project.init();
        CGeneratorTask generator = new CGeneratorTask();
        generator.setProject(project);

        if (args.length > 0) {
            generator.version = Integer.parseInt(args[0]);
        }

        if (args.length > 1) {
            generator.source = new File(args[1]);
        }

        if (args.length > 2) {
            generator.target = new File(args[2]);
        }

        generator.execute();
    }

    public void execute() throws BuildException {
        try {

            String sourceDir = source + "/src/main/java";

            System.out.println("Parsing source files in: " + sourceDir);

            JamServiceFactory jamServiceFactory = JamServiceFactory.getInstance();
            JamServiceParams params = jamServiceFactory.createServiceParams();
            File[] dirs = new File[] {
                new File(sourceDir)
            };
            params.includeSourcePattern(dirs, "**/*.java");
            JamService jam = jamServiceFactory.createService(params);

            {
                CHeadersGenerator script = new CHeadersGenerator();
                script.setJam(jam);
                script.setTargetDir(target + "/src/libopenwire");
                script.setOpenwireVersion(version);
                script.run();
            }
            {
                CSourcesGenerator script = new CSourcesGenerator();
                script.setJam(jam);
                script.setTargetDir(target + "/src/libopenwire");
                script.setOpenwireVersion(version);
                script.run();
            }

        } catch (Exception e) {
            throw new BuildException(e);
        }
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public File getSource() {
        return source;
    }

    public void setSource(File basedir) {
        this.source = basedir;
    }

    public File getTarget() {
        return target;
    }

    public void setTarget(File target) {
        this.target = target;
    }

}
