/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.activemq.karaf.commands;

import org.apache.felix.gogo.commands.Action;
import org.apache.felix.gogo.commands.Argument;
import org.apache.felix.gogo.commands.basic.AbstractCommand;
import org.apache.felix.gogo.commands.basic.ActionPreparator;
import org.apache.felix.gogo.commands.basic.DefaultActionPreparator;
import org.apache.karaf.shell.console.BlueprintContainerAware;
import org.apache.karaf.shell.console.BundleContextAware;
import org.apache.karaf.shell.console.CompletableFunction;
import org.apache.karaf.shell.console.Completer;
import org.apache.karaf.shell.console.commands.GenericType;
import org.osgi.framework.BundleContext;
import org.osgi.service.blueprint.container.BlueprintContainer;
import org.osgi.service.blueprint.container.Converter;
import org.osgi.service.command.CommandSession;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Base command to process options and wrap native ActiveMQ console commands.
 */
public class ActiveMQCommand extends AbstractCommand implements CompletableFunction
{
    protected BlueprintContainer blueprintContainer;
    protected Converter blueprintConverter;
    protected String actionId;
    protected List<Completer> completers;

    public void setBlueprintContainer(BlueprintContainer blueprintContainer) {
        this.blueprintContainer = blueprintContainer;
    }

    public void setBlueprintConverter(Converter blueprintConverter) {
        this.blueprintConverter = blueprintConverter;
    }

    public void setActionId(String actionId) {
        this.actionId = actionId;
    }

    public List<Completer> getCompleters() {
        return completers;
    }

    public void setCompleters(List<Completer> completers) {
        this.completers = completers;
    }

    @Override
    protected ActionPreparator getPreparator() throws Exception {
        return new ActiveMQActionPreparator();
    }

    class ActiveMQActionPreparator extends DefaultActionPreparator {
        @Override
        public boolean prepare(Action action, CommandSession session, List<Object> params) throws Exception
        {
            Map<Argument, Field> arguments = new HashMap<Argument, Field>();
            List<Argument> orderedArguments = new ArrayList<Argument>();
            // Introspect
            for (Class type = action.getClass(); type != null; type = type.getSuperclass()) {
                for (Field field : type.getDeclaredFields()) {
                    Argument argument = field.getAnnotation(Argument.class);
                    if (argument != null) {
                        arguments.put(argument, field);
                        int index = argument.index();
                        while (orderedArguments.size() <= index) {
                            orderedArguments.add(null);
                        }
                        if (orderedArguments.get(index) != null) {
                            throw new IllegalArgumentException("Duplicate argument index: " + index);
                        }
                        orderedArguments.set(index, argument);
                    }
                }
            }
            // Check indexes are correct
            for (int i = 0; i < orderedArguments.size(); i++) {
                if (orderedArguments.get(i) == null) {
                    throw new IllegalArgumentException("Missing argument for index: " + i);
                }
            }
            // Populate
            Map<Argument, Object> argumentValues = new HashMap<Argument, Object>();
            int argIndex = 0;
            for (Iterator<Object> it = params.iterator(); it.hasNext();) {
                Object param = it.next();
                if (argIndex >= orderedArguments.size()) {
                    throw new IllegalArgumentException("Too many arguments specified");
                }
                Argument argument = orderedArguments.get(argIndex);
                if (!argument.multiValued()) {
                    argIndex++;
                }
                if (argument.multiValued()) {
                    List<Object> l = (List<Object>) argumentValues.get(argument);
                    if (l == null) {
                        l = new ArrayList<Object>();
                        argumentValues.put(argument, l);
                    }
                    l.add(param);
                } else {
                    argumentValues.put(argument, param);
                }
            }

            for (Map.Entry<Argument, Object> entry : argumentValues.entrySet()) {
                Field field = arguments.get(entry.getKey());
                Object value = convert(action, session, entry.getValue(), field.getGenericType());
                field.setAccessible(true);
                field.set(action, value);
            }
            return true;
        }

        @Override
        protected Object convert(Action action, CommandSession commandSession, Object o, Type type) throws Exception {
            return blueprintConverter.convert(o, new GenericType(type));
        }
    }

    public Action createNewAction() {
        Action action = (Action) blueprintContainer.getComponentInstance(actionId);
        if (action instanceof BlueprintContainerAware) {
            ((BlueprintContainerAware) action).setBlueprintContainer(blueprintContainer);
        }
        if (action instanceof BundleContextAware) {
            BundleContext context = (BundleContext) blueprintContainer.getComponentInstance("blueprintBundleContext");
            ((BundleContextAware) action).setBundleContext(context);
        }
        return action;
    }

}
