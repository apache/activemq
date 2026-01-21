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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FieldDescriptor {

    public static final String STRING_TYPE = "string".intern();
    public static final String BOOL_TYPE = "bool".intern();
    public static final String BYTES_TYPE = "bytes".intern();
    public static final String DOUBLE_TYPE = "double".intern();
    public static final String FLOAT_TYPE = "float".intern();
    
    public static final String INT32_TYPE = "int32".intern();
    public static final String INT64_TYPE = "int64".intern();
    public static final String UINT32_TYPE = "uint32".intern();
    public static final String UINT64_TYPE = "uint64".intern();
    public static final String SINT32_TYPE = "sint32".intern();
    public static final String SINT64_TYPE = "sint64".intern();
    public static final String FIXED32_TYPE = "fixed32".intern();
    public static final String FIXED64_TYPE = "fixed64".intern();
    public static final String SFIXED32_TYPE = "sfixed32".intern();
    public static final String SFIXED64_TYPE = "sfixed64".intern();
    
    public static final String REQUIRED_RULE = "required".intern();
    public static final String OPTIONAL_RULE= "optional".intern();
    public static final String REPEATED_RULE = "repeated".intern();
    
    public static final Set<String> INT32_TYPES = new HashSet<String>();
    public static final Set<String> INT64_TYPES = new HashSet<String>();    
    public static final Set<String> INTEGER_TYPES = new HashSet<String>();
    public static final Set<String> NUMBER_TYPES = new HashSet<String>();
    public static final Set<String> SCALAR_TYPES = new HashSet<String>();
    
    public static final Set<String> SIGNED_TYPES = new HashSet<String>();
    public static final Set<String> UNSIGNED_TYPES = new HashSet<String>();
    
    static {
        INT32_TYPES.add(INT32_TYPE);
        INT32_TYPES.add(UINT32_TYPE);
        INT32_TYPES.add(SINT32_TYPE);
        INT32_TYPES.add(FIXED32_TYPE);
        INT32_TYPES.add(SFIXED32_TYPE);

        INT64_TYPES.add(INT64_TYPE);
        INT64_TYPES.add(UINT64_TYPE);
        INT64_TYPES.add(SINT64_TYPE);
        INT64_TYPES.add(FIXED64_TYPE);
        INT64_TYPES.add(SFIXED64_TYPE);
        
        INTEGER_TYPES.addAll(INT32_TYPES);
        INTEGER_TYPES.addAll(INT64_TYPES);
        
        NUMBER_TYPES.addAll(INTEGER_TYPES);
        NUMBER_TYPES.add(DOUBLE_TYPE);
        NUMBER_TYPES.add(FLOAT_TYPE);
        
        SCALAR_TYPES.addAll(NUMBER_TYPES);
        SCALAR_TYPES.add(STRING_TYPE);
        SCALAR_TYPES.add(BOOL_TYPE);
        SCALAR_TYPES.add(BYTES_TYPE);
    }
    
    
    private String name;
    private String type;
    private String rule;
    private int tag;
    private Map<String,OptionDescriptor> options;
    private TypeDescriptor typeDescriptor;
    private final MessageDescriptor parent;
    private MessageDescriptor group;

    public FieldDescriptor(MessageDescriptor parent) {
        this.parent = parent;
    }
    
    public void validate(List<String> errors) {
        if( group!=null ) {
            typeDescriptor=group;
        }
        if( !SCALAR_TYPES.contains(type) ) {
            // Find the type def for that guy..
            if( typeDescriptor==null ) {
                typeDescriptor = parent.getType(type);
            }
            if( typeDescriptor == null ) {
                typeDescriptor = parent.getProtoDescriptor().getType(type);
            }
            if( typeDescriptor == null ) {
                errors.add("Field type not found: "+type);
            }
        }
    }

    public boolean isGroup() {
        return group!=null;
    }

    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }

    public String getRule() {
        return rule;
    }
    public void setRule(String rule) {
        this.rule = rule.intern();
    }
    
    public boolean isOptional() {
        return this.rule == OPTIONAL_RULE;
    }
    public boolean isRequired() {
        return this.rule == REQUIRED_RULE;
    }
    public boolean isRepeated() {
        return this.rule == REPEATED_RULE;
    }

    public int getTag() {
        return tag;
    }
    public void setTag(int tag) {
        this.tag = tag;
    }

    public Map<String,OptionDescriptor> getOptions() {
        return options;
    }
    public void setOptions(Map<String,OptionDescriptor> options) {
        this.options = options;
    }

    public String getType() {
        return type;
    }
    public void setType(String type) {
        this.type = type.intern();
    }

    public boolean isMessageType() {
        return !SCALAR_TYPES.contains(type);
    }

    public boolean isScalarType() {
        return SCALAR_TYPES.contains(type);
    }

    public boolean isNumberType() {
        return NUMBER_TYPES.contains(type);
    }
    
    public boolean isIntegerType() {
        return INTEGER_TYPES.contains(type);
    }

    public boolean isInteger32Type() {
        return INT32_TYPES.contains(type);
    }

    public boolean isInteger64Type() {
        return INT64_TYPES.contains(type);
    }

    public boolean isStringType() {
        return type==STRING_TYPE;
    }

    public TypeDescriptor getTypeDescriptor() {
        return typeDescriptor;
    }

    public void setTypeDescriptor(TypeDescriptor typeDescriptor) {
        this.typeDescriptor = typeDescriptor;
    }

    public MessageDescriptor getGroup() {
        return group;
    }
    public void setGroup(MessageDescriptor group) {
        this.group = group;        
    }

}
