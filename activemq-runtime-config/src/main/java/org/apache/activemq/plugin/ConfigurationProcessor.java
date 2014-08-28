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
package org.apache.activemq.plugin;

import java.util.List;
import org.apache.activemq.schema.core.DtoBroker;

public interface ConfigurationProcessor {

    public void processChanges(List current, List modified);

    public void processChanges(DtoBroker current, DtoBroker modified);

    public void modify(Object existing, Object candidate);

    public void addNew(Object o);

    public void remove(Object o);

    public ConfigurationProcessor findProcessor(Object o);

}
