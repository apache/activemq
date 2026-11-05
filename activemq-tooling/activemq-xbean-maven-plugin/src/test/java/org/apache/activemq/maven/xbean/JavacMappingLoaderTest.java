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

import static org.apache.activemq.maven.xbean.SampleSupport.element;
import static org.apache.activemq.maven.xbean.SampleSupport.load;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.xbean.spring.generator.AttributeMapping;
import org.apache.xbean.spring.generator.ElementMapping;
import org.apache.xbean.spring.generator.InvalidModelException;
import org.apache.xbean.spring.generator.NamespaceMapping;
import org.apache.xbean.spring.generator.ParameterMapping;
import org.junit.BeforeClass;
import org.junit.Test;

/** Each test pins one of the QDox rules the schema depends on. */
public class JavacMappingLoaderTest {

    private static NamespaceMapping namespace;
    private static ElementMapping plainBean;

    @BeforeClass
    public static void loadSamples() throws Exception {
        namespace = load("src", Set.of("org.apache.activemq.xbean.sample.Excluded"));
        plainBean = element(namespace, "plainBean");
    }

    @Test
    public void onlyTopLevelTaggedNonAbstractTypesBecomeElements() {
        var names = new TreeSet<String>();
        for (Object element : namespace.getElements()) {
            names.add(((ElementMapping) element).getElementName());
        }
        assertEquals(new TreeSet<>(List.of("kind", "plainBean", "taggedInterface", "widget", "widgetFactory", "wrapped")), names);
        assertEquals("root element", plainBean, namespace.getRootElement());
    }

    @Test
    public void descriptionKeepsLinksParagraphsAndIndentedExamples() {
        String description = plainBean.getDescription();
        assertTrue(description, description.startsWith("A bean with a bit of everything. Links like {@link RootBean} and HTML such\nas <a href="));
        assertTrue("paragraph break kept", description.contains("description.\n\nParagraphs"));
        assertTrue("one space removed after the asterisk", description.contains("\n<pre class=\"code\">\n"));
        assertTrue("indented example keeps the rest of its indentation", description.contains("\n  <property name=\"name\" value=\"x\" />\n"));
        assertEquals("A small widget", element(namespace, "widget").getDescription());
    }

    @Test
    public void wrappedQuotedValueKeepsOnlyItsFirstLine() {
        assertEquals("Only the first line of a wrapped value", element(namespace, "wrapped").getDescription());
    }

    @Test
    public void attributesComeFromSettersOfAnyVisibilityUpTheHierarchy() {
        assertNotNull("subclass setter", plainBean.getAttribute("name"));
        assertNotNull("boolean is/set pair", plainBean.getAttribute("enabled"));
        assertNotNull("private setter counts", plainBean.getAttribute("secret"));
        assertNotNull("inherited from the abstract base", plainBean.getAttribute("baseValue"));
        assertNotNull("inherited from the root", plainBean.getAttribute("rootValue"));
        assertNull("getter only", plainBean.getAttribute("count"));
        assertNull("static", plainBean.getAttribute("ignored"));
        assertNull("hidden", plainBean.getAttribute("hiddenValue"));
        assertEquals("Introspector keeps acronyms", "DLQ", plainBean.getAttribute("DLQ").getAttributeName());
    }

    @Test
    public void attributeDescriptionPrefersTagThenGetterThenSetter() {
        assertEquals("The name of this bean; the getter comment wins over the setter comment.", plainBean.getAttribute("name").getDescription());
        assertEquals("Whether the bean is enabled", plainBean.getAttribute("enabled").getDescription());
        assertEquals("Set on the base class", plainBean.getAttribute("baseValue").getDescription());
        AttributeMapping renamed = plainBean.getAttribute("renamed");
        assertEquals("original", renamed.getPropertyName());
        assertEquals("An aliased attribute", renamed.getDescription());
    }

    @Test
    public void typesIgnoreGenericsAndHonourNestedTypeAndArrays() {
        AttributeMapping items = plainBean.getAttribute("items");
        assertTrue(items.getType().isCollection());
        assertEquals("java.util.List", items.getType().getName());
        assertEquals("org.apache.activemq.xbean.sample.Widget", items.getType().getNestedType().getName());
        assertEquals("java.lang.Object", plainBean.getAttribute("plainList").getType().getNestedType().getName());
        assertEquals("java.lang.String[]", plainBean.getAttribute("names").getType().getName());
        assertEquals("int[][]", plainBean.getAttribute("grid").getType().getName());
        assertFalse("a Map is not a Collection", plainBean.getAttribute("settings").getType().isCollection());
        assertEquals("java.util.Map", plainBean.getAttribute("settings").getType().getName());
        assertEquals("java.io.File", plainBean.getAttribute("directory").getType().getName());
        assertEquals("org.apache.activemq.xbean.sample.SizeEditor", plainBean.getAttribute("size").getPropertyEditor());
    }

    @Test
    public void lifecycleTagsAreReadFromTheTaggedClassOnly() {
        assertEquals("start", plainBean.getInitMethod());
        assertEquals("stop", plainBean.getDestroyMethod());
        assertNull(plainBean.getFactoryMethod());
    }

    @Test
    public void constructorsKeepSourceOrderAndDescribeParametersFromParamTags() {
        List<?> constructors = plainBean.getConstructors();
        assertEquals(2, constructors.size());
        var first = (List<?>) constructors.get(0);
        assertEquals("name", ((ParameterMapping) first.get(0)).getName());
        assertEquals("size", ((ParameterMapping) first.get(1)).getName());
        // size is also a property, so the setter's (empty) description wins over the @param text
        assertEquals("", plainBean.getAttribute("size").getDescription());
        var second = (List<?>) constructors.get(1);
        assertEquals("properties", ((ParameterMapping) second.get(0)).getName());
        assertEquals("java.util.Properties", ((ParameterMapping) second.get(0)).getType().getName());
        AttributeMapping properties = plainBean.getAttribute("properties");
        assertEquals("a parameter that is no property gets its @param text", "every setting, copied; not a property either", properties.getDescription());
        assertEquals("java.util.Properties", properties.getType().getName());
    }

    @Test
    public void hierarchyListsStopBeforeObjectAndCollectDirectInterfaces() {
        assertEquals(List.of("org.apache.activemq.xbean.sample.BaseBean", "org.apache.activemq.xbean.sample.RootBean"), plainBean.getSuperClasses());
        assertTrue(plainBean.getInterfaces().contains("java.io.Serializable"));
        assertTrue(plainBean.getInterfaces().contains("java.lang.Runnable"));
        assertTrue("enum walk stops at java.lang.Enum", element(namespace, "kind").getSuperClasses().isEmpty());
    }

    @Test
    public void interfaceAndFactoryBeanNaming() {
        assertNotNull(element(namespace, "taggedInterface").getAttribute("value"));
        assertEquals("org.apache.activemq.xbean.sample.WidgetFactoryBean", element(namespace, "widgetFactory").getClassName());
    }

    @Test
    public void modernSyntaxParses() throws Exception {
        NamespaceMapping modern = load("modern", Set.of());
        assertNotNull(element(modern, "point").getAttribute("label"));
        assertTrue(element(modern, "circle").getInterfaces().contains("org.apache.activemq.xbean.modern.Shape"));
        assertNotNull(element(modern, "shape").getAttribute("name"));
    }

    @Test
    public void hiddenPropertyUsedByAConstructorIsAModelError() {
        InvalidModelException thrown = assertThrows(InvalidModelException.class, () -> load("invalid", Set.of()));
        assertTrue(thrown.getMessage(), thrown.getMessage().contains("token"));
    }
}
