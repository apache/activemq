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
package org.apache.activemq.maven;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.activemq.console.Main;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;

/**
 * Goal which starts activemq broker.
 * 
 * @goal broker
 * @phase process-sources
 */
public class ServerMojo extends AbstractMojo {
    /**
     * Location of the output directory. Defaults to target.
     * 
     * @parameter expression="${project.build.directory}"
     * @required
     */
    private File outputDirectory;

    /**
     * Location of the predefined config files.
     * 
     * @parameter expression="${configDirectory}"
     *            default-value="${basedir}/src/main/resources/broker-conf"
     * @required
     */
    private String configDirectory;

    /**
     * Type of activemq configuration to use. This is also the filename used.
     * 
     * @parameter expression="${configType}" default-value="activemq"
     * @required
     */
    private String configType;

    /**
     * Location of activemq config file other those found in resources/config.
     * 
     * @parameter expression="${configFile}"
     */
    private File configFile;

    /**
     * Broker URL.
     * 
     * @parameter expression="${url}"
     */
    private String url;

    public void execute() throws MojoExecutionException {

        File out = outputDirectory;

        // Create output directory if it doesn't exist.
        if (!out.exists()) {
            out.mkdirs();
        }

        String[] args = new String[2];
        if (url != null) {
            args[0] = "start";
            args[1] = url;
        } else {
            File config;
            if (configFile != null) {
                config = configFile;
            } else {

                config = new File(configDirectory + File.separator + configType + ".xml");
            }

            try {
                config = copy(config);
            } catch (IOException e) {
                throw new MojoExecutionException(e.getMessage());
            }
            args[0] = "start";
            args[1] = "xbean:" + (config.toURI()).toString();
        }

        Main.main(args);
    }

    /**
     * Copy activemq configuration file to output directory.
     * 
     * @param source
     * @return
     * @throws IOException
     */
    public File copy(File source) throws IOException {
        FileChannel in = null;
        FileChannel out = null;

        File dest = new File(outputDirectory.getAbsolutePath() + File.separator + "activemq.xml");

        try {
            in = new FileInputStream(source).getChannel();
            out = new FileOutputStream(dest).getChannel();

            long size = in.size();
            MappedByteBuffer buf = in.map(FileChannel.MapMode.READ_ONLY, 0, size);

            out.write(buf);

        } finally {
            if (in != null) {
                in.close();
            }
            if (out != null) {
                out.close();
            }
        }

        return dest;
    }
}
