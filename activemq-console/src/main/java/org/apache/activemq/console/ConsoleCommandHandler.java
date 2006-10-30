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
package org.apache.activemq.console;

import org.apache.activemq.broker.util.CommandHandler;
import org.apache.activemq.console.command.ShellCommand;
import org.apache.activemq.console.formatter.GlobalWriter;
import org.apache.activemq.console.formatter.CommandShellOutputFormatter;

import javax.jms.TextMessage;
import java.util.List;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.io.ByteArrayOutputStream;

/**
 * A default implementation of the @{link CommandHandler} interface
 *
 * @version $Revision: $
 */
public class ConsoleCommandHandler implements CommandHandler {

    private ShellCommand command = new ShellCommand();

    public void processCommand(TextMessage request, TextMessage response) throws Exception {

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        GlobalWriter.instantiate(new CommandShellOutputFormatter(out));

        // lets turn the text into a list of arguments
        List tokens = tokenize(request.getText());
        command.execute(tokens);

        out.flush();
        byte[] bytes = out.toByteArray();

        String text = new String(bytes);
        response.setText(text);
    }

    protected List tokenize(String text) {
        List answer = new ArrayList();
        StringTokenizer iter = new StringTokenizer(text);
        while (iter.hasMoreTokens()) {
            answer.add(iter.nextToken());
        }
        return answer;
    }
}
