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
package org.apache.activemq.systest.task;

import org.codehaus.jam.JAnnotation;
import org.codehaus.jam.JAnnotationValue;
import org.codehaus.jam.JClass;
import org.codehaus.jam.JComment;
import org.codehaus.jam.JConstructor;
import org.codehaus.jam.JElement;
import org.codehaus.jam.JField;
import org.codehaus.jam.JMethod;
import org.codehaus.jam.JPackage;
import org.codehaus.jam.JProperty;
import org.codehaus.jam.JSourcePosition;
import org.codehaus.jam.JamClassLoader;
import org.codehaus.jam.visitor.JVisitor;

/**
 * 
 * @version $Revision: 1.1 $
 */
public class ScenarioJClassStub implements JClass {
    protected static final String[] EMPTY_ARRAY = {};

    private final String qualifiedName;
    private final String[] interfaceNames;
    private final String simpleName;
    private JClass[] interfaces;

    public ScenarioJClassStub(String qualifiedName, String[] interfaceNames) {
        this.qualifiedName = qualifiedName;
        this.interfaceNames = interfaceNames;

        int idx = qualifiedName.lastIndexOf('.');
        if (idx > 0) {
            this.simpleName = qualifiedName.substring(idx + 1);
        }
        else {
            this.simpleName = qualifiedName;
        }
    }

    public JPackage getContainingPackage() {
        return null;
    }

    public JClass getSuperclass() {
        return null;
    }

    public JClass[] getInterfaces() {
        if (interfaces == null) {
            int size = interfaceNames.length;
            interfaces = new JClass[size];
            for (int i = 0; i < interfaceNames.length; i++) {
                interfaces[i] = new ScenarioJClassStub(interfaceNames[i], EMPTY_ARRAY);
            }
        }
        return interfaces;
    }

    public JField[] getFields() {
        return null;
    }

    public JField[] getDeclaredFields() {
        return null;
    }

    public JMethod[] getMethods() {
        return null;
    }

    public JMethod[] getDeclaredMethods() {
        return null;
    }

    public JConstructor[] getConstructors() {
        return null;
    }

    public JProperty[] getProperties() {
        return null;
    }

    public JProperty[] getDeclaredProperties() {
        return null;
    }

    public boolean isInterface() {
        return false;
    }

    public boolean isAnnotationType() {
        return false;
    }

    public boolean isPrimitiveType() {
        return false;
    }

    public boolean isBuiltinType() {
        return false;
    }

    public Class getPrimitiveClass() {
        return null;
    }

    public boolean isFinal() {
        return false;
    }

    public boolean isStatic() {
        return false;
    }

    public boolean isAbstract() {
        return false;
    }

    public boolean isVoidType() {
        return false;
    }

    public boolean isObjectType() {
        return false;
    }

    public boolean isArrayType() {
        return false;
    }

    public JClass getArrayComponentType() {
        return null;
    }

    public int getArrayDimensions() {
        return 0;
    }

    public boolean isAssignableFrom(JClass arg0) {
        return false;
    }

    public JClass[] getClasses() {
        return null;
    }

    public JClass getContainingClass() {
        return null;
    }

    public String getFieldDescriptor() {
        return null;
    }

    public boolean isEnumType() {
        return false;
    }

    public JamClassLoader getClassLoader() {
        return null;
    }

    public JClass forName(String arg0) {
        return null;
    }

    public JClass[] getImportedClasses() {
        return null;
    }

    public JPackage[] getImportedPackages() {
        return null;
    }

    public boolean isUnresolvedType() {
        return false;
    }

    public int getModifiers() {
        return 0;
    }

    public boolean isPackagePrivate() {
        return false;
    }

    public boolean isPrivate() {
        return false;
    }

    public boolean isProtected() {
        return false;
    }

    public boolean isPublic() {
        return false;
    }

    public JAnnotation[] getAnnotations() {
        return null;
    }

    public JAnnotation getAnnotation(Class arg0) {
        return null;
    }

    public Object getAnnotationProxy(Class arg0) {
        return null;
    }

    public JAnnotation getAnnotation(String arg0) {
        return null;
    }

    public JAnnotationValue getAnnotationValue(String arg0) {
        return null;
    }

    public JComment getComment() {
        return null;
    }

    public JAnnotation[] getAllJavadocTags() {
        return null;
    }

    public JElement getParent() {
        return null;
    }

    public String getSimpleName() {
        return simpleName;
    }

    public String getQualifiedName() {
        return qualifiedName;
    }

    public JSourcePosition getSourcePosition() {
        return null;
    }

    public void accept(JVisitor arg0) {
    }

    public Object getArtifact() {
        return null;
    }

}
