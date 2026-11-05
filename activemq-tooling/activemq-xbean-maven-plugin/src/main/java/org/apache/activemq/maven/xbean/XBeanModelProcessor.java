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
package org.apache.activemq.maven.xbean;

import java.beans.Introspector;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.NestingKind;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.ArrayType;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.ElementFilter;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;

import com.sun.source.tree.ClassTree;
import com.sun.source.util.Trees;

import org.apache.xbean.spring.generator.AttributeMapping;
import org.apache.xbean.spring.generator.ElementMapping;
import org.apache.xbean.spring.generator.InvalidModelException;
import org.apache.xbean.spring.generator.MapMapping;
import org.apache.xbean.spring.generator.ParameterMapping;
import org.apache.xbean.spring.generator.Type;
import org.apache.xbean.spring.generator.Utils;

/**
 * Builds xbean {@link ElementMapping}s from the classes javac hands to it.
 *
 * <p>The rules are those of xbean's {@code QdoxMappingLoader}, kept on purpose
 * where they look odd, because the generated schema is consumed by JAXB in
 * activemq-runtime-config and validated against by every broker XML file. In
 * particular: only an explicit {@code abstract} keyword disqualifies a type, so a
 * tagged interface becomes an element; bean properties come from declared
 * methods of any visibility; lifecycle tags are only read from the tagged class
 * itself; generic type arguments are ignored and only the {@code nestedType}
 * tag names a collection's child type.
 */
@SupportedAnnotationTypes("*")
public class XBeanModelProcessor extends AbstractProcessor {

    static final String XBEAN_TAG = "org.apache.xbean.XBean";
    static final String PROPERTY_TAG = "org.apache.xbean.Property";
    static final String INIT_METHOD_TAG = "org.apache.xbean.InitMethod";
    static final String DESTROY_METHOD_TAG = "org.apache.xbean.DestroyMethod";
    static final String FACTORY_METHOD_TAG = "org.apache.xbean.FactoryMethod";
    static final String MAP_TAG = "org.apache.xbean.Map";
    static final String FLAT_PROPERTY_TAG = "org.apache.xbean.Flat";
    static final String FLAT_COLLECTION_TAG = "org.apache.xbean.FlatCollection";

    private final String defaultNamespace;
    private final Set<String> excludedClasses;
    private final Consumer<String> info;
    private final Consumer<String> debug;
    private final List<ElementMapping> elements = new ArrayList<>();

    private Elements elementUtils;
    private Types typeUtils;
    private Trees trees;
    private TypeMirror collectionType;

    public XBeanModelProcessor(String defaultNamespace, Set<String> excludedClasses, Consumer<String> info, Consumer<String> debug) {
        this.defaultNamespace = defaultNamespace;
        this.excludedClasses = excludedClasses;
        this.info = info;
        this.debug = debug;
    }

    /** every element found so far, in the order the classes were visited */
    public List<ElementMapping> getElements() {
        return Collections.unmodifiableList(elements);
    }

    @Override
    public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.latestSupported();
    }

    @Override
    public synchronized void init(ProcessingEnvironment env) {
        super.init(env);
        elementUtils = env.getElementUtils();
        typeUtils = env.getTypeUtils();
        trees = Trees.instance(env);
        collectionType = typeUtils.erasure(elementUtils.getTypeElement("java.util.Collection").asType());
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        if (roundEnv.processingOver()) {
            return false;
        }
        for (Element root : roundEnv.getRootElements()) {
            if (root instanceof TypeElement type && type.getNestingKind() == NestingKind.TOP_LEVEL) {
                visit(type);
            }
        }
        return false;
    }

    private void visit(TypeElement type) {
        String className = type.getQualifiedName().toString();
        if (excludedClasses.contains(className)) {
            debug.accept("Excluded: " + className);
            return;
        }
        DocComment doc = DocComment.parse(elementUtils.getDocComment(type));
        DocComment.Tag xbeanTag = doc.tag(XBEAN_TAG);
        if (xbeanTag == null) {
            debug.accept("No XML annotation found for type: " + className);
            return;
        }
        ClassTree tree = trees.getTree(type);
        if (tree != null && tree.getModifiers().getFlags().contains(Modifier.ABSTRACT)) {
            debug.accept("Skipping abstract type: " + className);
            return;
        }
        elements.add(loadElement(type, doc, xbeanTag));
    }

    private ElementMapping loadElement(TypeElement type, DocComment doc, DocComment.Tag xbeanTag) {
        String className = type.getQualifiedName().toString();
        String element = elementName(type, xbeanTag);
        String description = xbeanTag.parameter("description");
        if (description == null) {
            description = doc.body();
        }
        String namespace = parameter(xbeanTag, "namespace", defaultNamespace);
        boolean root = xbeanTag.booleanParameter("rootElement");
        String contentProperty = xbeanTag.parameter("contentProperty");
        String factoryClass = xbeanTag.parameter("factoryClass");

        var mapsByPropertyName = new HashMap<String, MapMapping>();
        var flatProperties = new ArrayList<String>();
        var flatCollections = new HashMap<String, String>();
        var attributes = new HashSet<AttributeMapping>();
        var attributesByPropertyName = new HashMap<String, AttributeMapping>();

        for (TypeElement c = type; c != null; c = superclassOf(c)) {
            for (BeanProperty property : beanProperties(c).values()) {
                // only properties with a setter can be configured
                if (property.mutator == null) {
                    continue;
                }
                AttributeMapping attribute = loadAttribute(property, "");
                if (attribute != null) {
                    attributes.add(attribute);
                    attributesByPropertyName.put(attribute.getPropertyName(), attribute);
                }
                if (property.accessor != null) {
                    DocComment accessorDoc = docOf(property.accessor);
                    DocComment.Tag mapTag = accessorDoc.tag(MAP_TAG);
                    if (mapTag != null) {
                        mapsByPropertyName.put(property.name, new MapMapping(
                                mapTag.parameter("entryName"),
                                mapTag.parameter("keyName"),
                                Boolean.parseBoolean(mapTag.parameter("flat")),
                                mapTag.parameter("dups"),
                                mapTag.parameter("defaultKey")));
                    }
                    DocComment.Tag flatCollectionTag = accessorDoc.tag(FLAT_COLLECTION_TAG);
                    if (flatCollectionTag != null) {
                        String childName = flatCollectionTag.parameter("childElement");
                        if (childName == null) {
                            throw new InvalidModelException("Flat collections must specify the childElement attribute.");
                        }
                        flatCollections.put(property.name, childName);
                    }
                    if (accessorDoc.tag(FLAT_PROPERTY_TAG) != null) {
                        flatProperties.add(property.name);
                    }
                }
            }
        }

        // lifecycle tags are only read from the tagged class, never from a superclass
        String initMethod = null;
        String destroyMethod = null;
        String factoryMethod = null;
        for (ExecutableElement method : ElementFilter.methodsIn(type.getEnclosedElements())) {
            if (!method.getModifiers().contains(Modifier.PUBLIC)) {
                continue;
            }
            DocComment methodDoc = docOf(method);
            String name = method.getSimpleName().toString();
            if (initMethod == null && methodDoc.tag(INIT_METHOD_TAG) != null) {
                initMethod = name;
            }
            if (destroyMethod == null && methodDoc.tag(DESTROY_METHOD_TAG) != null) {
                destroyMethod = name;
            }
            if (factoryMethod == null && methodDoc.tag(FACTORY_METHOD_TAG) != null) {
                factoryMethod = name;
            }
        }

        var constructorArgs = new ArrayList<List<ParameterMapping>>();
        for (Element member : type.getEnclosedElements()) {
            if (!(member instanceof ExecutableElement method) || !isValidConstructor(factoryMethod, method)) {
                continue;
            }
            var args = new ArrayList<ParameterMapping>();
            for (VariableElement parameter : method.getParameters()) {
                String parameterName = parameter.getSimpleName().toString();
                AttributeMapping attribute = attributesByPropertyName.get(parameterName);
                if (attribute == null) {
                    attribute = loadParameter(type, method, parameter);
                    attributes.add(attribute);
                    attributesByPropertyName.put(attribute.getPropertyName(), attribute);
                }
                args.add(new ParameterMapping(attribute.getPropertyName(), toMappingType(parameter.asType(), null)));
            }
            constructorArgs.add(Collections.unmodifiableList(args));
        }

        var interfaces = new HashSet<String>(qualifiedNames(type.getInterfaces()));
        TypeElement actualClass = type;
        if (factoryClass != null) {
            TypeElement factory = elementUtils.getTypeElement(factoryClass);
            if (factory != null) {
                info.accept("Detected factory: using " + factoryClass + " instead of " + className);
                actualClass = factory;
            } else {
                info.accept("Could not load class built by factory: " + factoryClass);
            }
        }
        var superClasses = new ArrayList<String>();
        if (actualClass != type) {
            superClasses.add(actualClass.getQualifiedName().toString());
        }
        for (TypeElement s = superclassOf(actualClass); s != null; s = superclassOf(s)) {
            superClasses.add(s.getQualifiedName().toString());
            interfaces.addAll(qualifiedNames(s.getInterfaces()));
        }

        return new ElementMapping(namespace,
                element,
                className,
                description,
                root,
                initMethod,
                destroyMethod,
                factoryMethod,
                contentProperty,
                attributes,
                constructorArgs,
                flatProperties,
                mapsByPropertyName,
                flatCollections,
                superClasses,
                interfaces);
    }

    private static String elementName(TypeElement type, DocComment.Tag tag) {
        String elementName = tag.parameter("element");
        if (elementName == null) {
            String className = type.getSimpleName().toString();
            // strip off "Bean" from a spring factory bean
            if (className.endsWith("FactoryBean")) {
                className = className.substring(0, className.length() - 4);
            }
            elementName = Utils.decapitalise(className);
        }
        return elementName;
    }

    private AttributeMapping loadAttribute(BeanProperty property, String defaultDescription) {
        DocComment.Tag propertyTag = propertyTag(property);
        if (propertyTag != null && propertyTag.booleanParameter("hidden")) {
            return null;
        }
        String attribute = parameter(propertyTag, "alias", property.name);
        String description = attributeDescription(property, propertyTag, defaultDescription);
        String defaultValue = parameter(propertyTag, "default", null);
        boolean fixed = propertyTag != null && propertyTag.booleanParameter("fixed");
        boolean required = propertyTag != null && propertyTag.booleanParameter("required");
        String nestedType = parameter(propertyTag, "nestedType", null);
        String propertyEditor = parameter(propertyTag, "propertyEditor", null);
        return new AttributeMapping(attribute,
                property.name,
                description,
                toMappingType(property.type, nestedType),
                defaultValue,
                fixed,
                required,
                propertyEditor);
    }

    private DocComment.Tag propertyTag(BeanProperty property) {
        if (property.accessor != null) {
            DocComment.Tag tag = docOf(property.accessor).tag(PROPERTY_TAG);
            if (tag != null) {
                return tag;
            }
        }
        if (property.mutator != null) {
            return docOf(property.mutator).tag(PROPERTY_TAG);
        }
        return null;
    }

    private String attributeDescription(BeanProperty property, DocComment.Tag propertyTag, String defaultDescription) {
        String description = parameter(propertyTag, "description", null);
        if (description != null && !description.trim().isEmpty()) {
            return description.trim();
        }
        if (property.accessor != null) {
            description = docOf(property.accessor).body();
            if (description != null && !description.trim().isEmpty()) {
                return description.trim();
            }
        }
        if (property.mutator != null) {
            description = docOf(property.mutator).body();
            if (description != null && !description.trim().isEmpty()) {
                return description.trim();
            }
        }
        return defaultDescription;
    }

    private AttributeMapping loadParameter(TypeElement type, ExecutableElement method, VariableElement parameter) {
        String parameterName = parameter.getSimpleName().toString();
        String parameterDescription = parameterDescription(method, parameterName);
        // first attempt to load the attribute from the java beans accessor methods of this class
        BeanProperty property = beanProperties(type).get(parameterName);
        if (property != null) {
            AttributeMapping attribute = loadAttribute(property, parameterDescription);
            if (attribute == null) {
                throw new InvalidModelException("Hidden property usage: The construction method "
                        + type.getQualifiedName() + "." + method.getSimpleName() + " can not use a hidden property " + parameterName);
            }
            return attribute;
        }
        return new AttributeMapping(parameterName,
                parameterName,
                parameterDescription,
                toMappingType(parameter.asType(), null),
                null,
                false,
                false,
                null);
    }

    private String parameterDescription(ExecutableElement method, String parameterName) {
        for (DocComment.Tag tag : docOf(method).tags("param")) {
            List<String> words = TagParameters.parseWords(tag.value());
            if (!words.isEmpty() && words.get(0).equals(parameterName)) {
                String description = tag.value().trim();
                if (description.startsWith(parameterName)) {
                    description = description.substring(parameterName.length()).trim();
                }
                return description;
            }
        }
        return null;
    }

    private static boolean isValidConstructor(String factoryMethod, ExecutableElement method) {
        if (!method.getModifiers().contains(Modifier.PUBLIC) || method.getParameters().isEmpty()) {
            return false;
        }
        if (factoryMethod == null) {
            return method.getKind() == javax.lang.model.element.ElementKind.CONSTRUCTOR;
        }
        return method.getSimpleName().contentEquals(factoryMethod);
    }

    /**
     * Bean properties declared on this one type, in declaration order. Getter
     * and setter of any visibility count; the type is that of whichever was
     * declared last, as QDox had it.
     */
    private Map<String, BeanProperty> beanProperties(TypeElement type) {
        var properties = new LinkedHashMap<String, BeanProperty>();
        for (ExecutableElement method : ElementFilter.methodsIn(type.getEnclosedElements())) {
            if (method.getModifiers().contains(Modifier.STATIC)) {
                continue;
            }
            String name = method.getSimpleName().toString();
            int parameterCount = method.getParameters().size();
            if (parameterCount == 0 && (startsWithPrefix(name, "is") || startsWithPrefix(name, "get"))) {
                BeanProperty property = properties.computeIfAbsent(propertyName(name), BeanProperty::new);
                property.accessor = method;
                property.type = method.getReturnType();
            } else if (parameterCount == 1 && startsWithPrefix(name, "set")) {
                BeanProperty property = properties.computeIfAbsent(propertyName(name), BeanProperty::new);
                property.mutator = method;
                property.type = method.getParameters().get(0).asType();
            }
        }
        return properties;
    }

    private static boolean startsWithPrefix(String name, String prefix) {
        return name.startsWith(prefix) && name.length() > prefix.length() && Character.isUpperCase(name.charAt(prefix.length()));
    }

    private static String propertyName(String methodName) {
        int start = methodName.startsWith("is") ? 2 : 3;
        return Introspector.decapitalize(methodName.substring(start));
    }

    private Type toMappingType(TypeMirror type, String nestedType) {
        if (type.getKind() == TypeKind.ARRAY) {
            int dimensions = 0;
            TypeMirror component = type;
            while (component.getKind() == TypeKind.ARRAY) {
                dimensions++;
                component = ((ArrayType) component).getComponentType();
            }
            return Type.newArrayType(nameOf(component), dimensions);
        }
        if (type.getKind() == TypeKind.DECLARED && typeUtils.isAssignable(typeUtils.erasure(type), collectionType)) {
            String nested = nestedType != null ? nestedType : "java.lang.Object";
            return Type.newCollectionType(nameOf(type), Type.newSimpleType(nested));
        }
        return Type.newSimpleType(nameOf(type));
    }

    /** the name QDox reported for a type: qualified for classes, bare for primitives and type variables */
    private static String nameOf(TypeMirror type) {
        switch (type.getKind()) {
        case DECLARED:
            return ((TypeElement) ((DeclaredType) type).asElement()).getQualifiedName().toString();
        case TYPEVAR:
            return type.toString();
        default:
            return type.toString();
        }
    }

    private static List<String> qualifiedNames(List<? extends TypeMirror> types) {
        var names = new ArrayList<String>();
        for (TypeMirror type : types) {
            if (type.getKind() == TypeKind.DECLARED) {
                names.add(nameOf(type));
            }
        }
        return names;
    }

    /** the superclass to keep walking into, or null at Object and at the enum and record roots */
    private static TypeElement superclassOf(TypeElement type) {
        TypeMirror superclass = type.getSuperclass();
        if (superclass.getKind() != TypeKind.DECLARED) {
            return null;
        }
        var element = (TypeElement) ((DeclaredType) superclass).asElement();
        String name = element.getQualifiedName().toString();
        if (name.equals("java.lang.Object") || name.equals("java.lang.Enum") || name.equals("java.lang.Record")) {
            return null;
        }
        return element;
    }

    private DocComment docOf(Element element) {
        return DocComment.parse(elementUtils.getDocComment(element));
    }

    private static String parameter(DocComment.Tag tag, String name, String defaultValue) {
        String value = tag != null ? tag.parameter(name) : null;
        return value != null ? value : defaultValue;
    }

    private static final class BeanProperty {
        final String name;
        ExecutableElement accessor;
        ExecutableElement mutator;
        TypeMirror type;

        BeanProperty(String name) {
            this.name = name;
        }
    }
}
