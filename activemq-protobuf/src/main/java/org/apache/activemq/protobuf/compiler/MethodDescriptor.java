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

public class MethodDescriptor {

    private final ProtoDescriptor protoDescriptor;
    private String name;
    private String parameter;
    private String returns;

    public MethodDescriptor(ProtoDescriptor protoDescriptor) {
        this.protoDescriptor = protoDescriptor;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setParameter(String parameter) {
        this.parameter = parameter;
    }

    public void setReturns(String returns) {
        this.returns = returns;
    }

    public ProtoDescriptor getProtoDescriptor() {
        return protoDescriptor;
    }

    public String getName() {
        return name;
    }

    public String getParameter() {
        return parameter;
    }

    public String getReturns() {
        return returns;
    }

}
