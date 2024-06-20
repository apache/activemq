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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class EnumDescriptor implements TypeDescriptor {

    private String name;
    private Map<String,EnumFieldDescriptor> fields= new LinkedHashMap<String, EnumFieldDescriptor>();
    private final ProtoDescriptor protoDescriptor;
    private final MessageDescriptor parent;
    private Map<String, OptionDescriptor> options = new LinkedHashMap<String, OptionDescriptor>();

    public EnumDescriptor(ProtoDescriptor protoDescriptor, MessageDescriptor parent) {
        this.protoDescriptor = protoDescriptor;
        this.parent = parent;
    }

    public String getName() {
        return name;
    }

    public Map<String,EnumFieldDescriptor> getFields() {
        return fields;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setFields(Map<String,EnumFieldDescriptor> fields) {
        this.fields = fields;
    }
    public ProtoDescriptor getProtoDescriptor() {
        return protoDescriptor;
    }

    private String getOption(Map<String, OptionDescriptor> options, String optionName, String defaultValue) {
        OptionDescriptor optionDescriptor = options.get(optionName);
        if (optionDescriptor == null) {
            return defaultValue;
        }
        return optionDescriptor.getValue();
    }
    
    private String constantToUCamelCase(String name) {
        boolean upNext=true;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if( Character.isJavaIdentifierPart(c) && Character.isLetterOrDigit(c)) {
                if( upNext ) {
                    c = Character.toUpperCase(c);
                    upNext=false;
                } else {
                    c = Character.toLowerCase(c);
                }
                sb.append(c);
            } else {
                upNext=true;
            }
        }
        return sb.toString();
    }
    
    public void validate(List<String> errors) {
        String createMessage = getOption(getOptions(), "java_create_message", null);        
        if( "true".equals(createMessage) ) {
            for (EnumFieldDescriptor field : getFields().values()) {
                String type = constantToUCamelCase(field.getName());
                
                TypeDescriptor typeDescriptor=null;
                // Find the type def for that guy..
                if( parent!=null ) {
                    typeDescriptor = parent.getType(type);
                }
                if( typeDescriptor == null ) {
                    typeDescriptor = protoDescriptor.getType(type);
                }
                if( typeDescriptor == null ) {
                    errors.add("ENUM constant '"+field.getName()+"' did not find expected associated message: "+type);
                } else {
                    field.associate(typeDescriptor);
                    typeDescriptor.associate(field);
                }
            }
        }
    }

    public MessageDescriptor getParent() {
        return parent;
    }

    public String getQName() {
        if( parent==null ) {
            return name;
        } else {
            return parent.getQName()+"."+name;
        }
    }

    public boolean isEnum() {
        return true;
    }

    public Map<String, OptionDescriptor> getOptions() {
        return options;
    }

    public void setOptions(Map<String, OptionDescriptor> options) {
        this.options = options;
    }

    public void associate(EnumFieldDescriptor desc) {
        throw new RuntimeException("not supported.");
    }


}
