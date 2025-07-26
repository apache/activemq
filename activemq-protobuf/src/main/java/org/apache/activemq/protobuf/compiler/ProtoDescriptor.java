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
package org.apache.activemq.protobuf.compiler;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ProtoDescriptor {

    private String packageName;
    private Map<String, OptionDescriptor> options = new LinkedHashMap<String, OptionDescriptor>();
    private Map<String, MessageDescriptor> messages = new LinkedHashMap<String, MessageDescriptor>();
    private Map<String, EnumDescriptor> enums = new LinkedHashMap<String, EnumDescriptor>();
    private List<MessageDescriptor> extendsList = new ArrayList<MessageDescriptor>();
    private Map<String, ServiceDescriptor> services = new LinkedHashMap<String, ServiceDescriptor>();
    List<String> imports = new ArrayList<String>();
    Map<String,ProtoDescriptor> importProtoDescriptors = new LinkedHashMap<String, ProtoDescriptor>();
    private String name;
    
    public void setPackageName(String packageName) {
        this.packageName = packageName;
    }

    public void setOptions(Map<String,OptionDescriptor> options) {
        this.options = options;
    }

    public void setMessages(Map<String,MessageDescriptor> messages) {
        this.messages = messages;
    }

    public void setEnums(Map<String,EnumDescriptor> enums) {
        this.enums = enums;
    }

    public void setExtends(List<MessageDescriptor> extendsList) {
        this.extendsList = extendsList;
    }

    public List<MessageDescriptor> getExtends() {
        return extendsList;
    }

    public String getPackageName() {
        return packageName;
    }

    public Map<String, OptionDescriptor> getOptions() {
        return options;
    }

    public Map<String,MessageDescriptor> getMessages() {
        return messages;
    }

    public Map<String,EnumDescriptor> getEnums() {
        return enums;
    }

    public void setServices(Map<String,ServiceDescriptor> services) {
        this.services = services;
    }

    public Map<String,ServiceDescriptor> getServices() {
        return services;
    }

    /**
     * Checks for validation errors in the proto definition and fills them 
     * into the errors list.
     * 
     * @return
     */
    public void validate(List<String> errors) {
        for (ProtoDescriptor o : importProtoDescriptors.values()) {
            o.validate(errors);
        }
        for (OptionDescriptor o : options.values()) {
            o.validate(errors);
        }
        for (MessageDescriptor o : messages.values()) {
            o.validate(errors);
        }
        for (EnumDescriptor o : enums.values()) {
            o.validate(errors);
        }
        for (MessageDescriptor o : extendsList) {
            o.validate(errors);
        }
        for (ServiceDescriptor o : services.values()) {
            o.validate(errors);
        }
    }

    public List<String> getImports() {
        return imports;
    }

    public void setImports(List<String> imports) {
        this.imports = imports;
    }

    public Map<String, ProtoDescriptor> getImportProtoDescriptors() {
        return importProtoDescriptors;
    }

    public void setImportProtoDescriptors(Map<String, ProtoDescriptor> importProtoDescriptors) {
        this.importProtoDescriptors = importProtoDescriptors;
    }

    public TypeDescriptor getType(String type) {
        for (MessageDescriptor o : messages.values()) {
            if( type.equals(o.getName()) ) {
                return o;
            }
            if( type.startsWith(o.getName()+".") ) {
                return o.getType( type.substring(o.getName().length()+1) );
            }
        }
        for (EnumDescriptor o : enums.values()) {
            if( type.equals(o.getName()) ) {
                return o;
            }
        }
        // Check to see if the type was qualified with the package name...
        for (ProtoDescriptor o : importProtoDescriptors.values()) {
            if( o.getPackageName()!=null && type.startsWith(o.getPackageName()+".") ) {
                return o.getType( type.substring(o.getPackageName().length()+1) );
            }
        }
        for (ProtoDescriptor o : importProtoDescriptors.values()) {
            TypeDescriptor rc = o.getType(type);
            if (rc != null) {
                return rc;
            }
        }
        return null;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

}
