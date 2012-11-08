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
package org.apache.activemq.console.command;

import org.apache.activemq.console.CommandContext;
import org.apache.activemq.console.command.store.StoreExporter;
import org.apache.activemq.console.command.store.amq.CommandLineSupport;

import java.util.Arrays;
import java.util.List;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class StoreExportCommand implements Command {

    private CommandContext context;

    @Override
    public void setCommandContext(CommandContext context) {
        this.context = context;
    }

    @Override
    public String getName() {
        return "export";
    }

    @Override
    public String getOneLineDescription() {
        return "Exports a stopped brokers data files to an archive file";
    }

    @Override
    public void execute(List<String> tokens) throws Exception {
        StoreExporter exporter = new StoreExporter();
        String[] remaining = CommandLineSupport.setOptions(exporter, tokens.toArray(new String[tokens.size()]));
        if (remaining.length > 0) {
          throw new Exception("Unexpected arguments: " + Arrays.asList(remaining));
        }
        exporter.execute();
    }
}
