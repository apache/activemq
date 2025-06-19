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

public class EnumFieldDescriptor {

    private String name;
    private int value;
    private final EnumDescriptor parent;
    private TypeDescriptor associatedType;
    
    public EnumFieldDescriptor(EnumDescriptor parent) {
        this.parent = parent;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public int getValue() {
        return value;
    }

    public EnumDescriptor getParent() {
        return parent;
    }

    public TypeDescriptor getAssociatedType() {
        return associatedType;
    }

    public void associate(TypeDescriptor associatedType) {
        this.associatedType = associatedType;
    }

}
