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

import java.beans.PropertyEditorManager;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.maven.artifact.Artifact;
import org.apache.maven.model.Resource;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Component;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.plugins.annotations.ResolutionScope;
import org.apache.maven.project.MavenProject;
import org.apache.maven.project.MavenProjectHelper;
import org.apache.xbean.spring.generator.DocumentationGenerator;
import org.apache.xbean.spring.generator.GeneratorPlugin;
import org.apache.xbean.spring.generator.LogFacade;
import org.apache.xbean.spring.generator.NamespaceMapping;
import org.apache.xbean.spring.generator.WikiDocumentationGenerator;
import org.apache.xbean.spring.generator.XmlMetadataGenerator;
import org.apache.xbean.spring.generator.XsdGenerator;

/**
 * Generates the xbean-spring mapping file, the XML schema and its documentation
 * from {@code @org.apache.xbean} javadoc tags. A drop in replacement for the
 * {@code mapping} goal of org.apache.xbean:maven-xbean-plugin that reads the
 * sources with javac rather than QDox and writes {@code META-INF/spring.handlers}
 * without a timestamp, so the output is reproducible.
 */
@Mojo(name = "generate", defaultPhase = LifecyclePhase.PROCESS_CLASSES, requiresDependencyResolution = ResolutionScope.COMPILE)
public class GenerateMojo extends AbstractMojo implements LogFacade {

    @Parameter(defaultValue = "${project}", readonly = true, required = true)
    private MavenProject project;

    @Component
    private MavenProjectHelper projectHelper;

    /** the XML namespace of the generated schema */
    @Parameter(required = true)
    private String namespace;

    /** the source directory of this project */
    @Parameter(defaultValue = "${basedir}/src/main/java")
    private File srcDir;

    /** further source directories to read, typically sibling modules */
    @Parameter
    private List<String> includes;

    /** extra directories or jars for resolving types and locating property editors */
    @Parameter
    private List<String> classPathIncludes;

    /** comma separated fully qualified names of top level classes to leave out */
    @Parameter
    private String excludedClasses;

    /** where the mapping file and META-INF entries are written */
    @Parameter(defaultValue = "${basedir}/target/xbean")
    private File outputDir;

    /** the schema file; defaults to {@code outputDir/artifactId.xsd} */
    @Parameter
    private File schema;

    /**
     * Packages searched for property editors, appended to the JDK search path.
     * A type with an editor becomes an attribute, one without becomes a nested
     * element. Unset by default: maven-xbean-plugin declared
     * {@code ${org.apache.xbean.spring.context.impl}} here, an expression that
     * never resolved, so its File, URI and ObjectName editors were never used and
     * the published schema has those properties as nested elements.
     */
    @Parameter
    private String propertyEditorPaths;

    /** attach the schema and its html documentation as build artifacts */
    @Parameter(defaultValue = "true")
    private boolean schemaAsArtifact;

    @Parameter(defaultValue = "true")
    private boolean generateSpringSchemasFile;

    @Parameter(defaultValue = "true")
    private boolean generateSpringHandlersFile;

    /** emit nested elements as an ordered xs:sequence rather than an unordered xs:choice */
    @Parameter(defaultValue = "true")
    private boolean strictXsdOrder;

    /** fail the build when javac reports errors while reading the sources */
    @Parameter(defaultValue = "false")
    private boolean failOnCompilerErrors;

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        if (schema == null) {
            schema = new File(outputDir, project.getArtifactId() + ".xsd");
        }
        getLog().debug("namespace[" + namespace + "] srcDir[" + srcDir + "] schema[" + schema + "] outputDir[" + outputDir
                + "] excludedClasses[" + excludedClasses + "] propertyEditorPaths[" + propertyEditorPaths + "]");

        if (propertyEditorPaths != null) {
            var editorSearchPath = new ArrayList<>(Arrays.asList(PropertyEditorManager.getEditorSearchPath()));
            for (StringTokenizer paths = new StringTokenizer(propertyEditorPaths, " ,"); paths.hasMoreTokens();) {
                editorSearchPath.add(paths.nextToken());
            }
            PropertyEditorManager.setEditorSearchPath(editorSearchPath.toArray(new String[0]));
        }

        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try {
            schema.getParentFile().mkdirs();
            outputDir.mkdirs();

            var loader = new JavacMappingLoader(namespace, sourceDirectories(), excludedClassNames(), javacClasspath(),
                    failOnCompilerErrors, getLog()::info, getLog()::debug);
            Set<NamespaceMapping> namespaces = loader.loadNamespaces();
            if (namespaces.isEmpty()) {
                getLog().warn("No xbean namespaces found");
            }

            // the generators decide between attribute and nested element by looking for a
            // property editor on the context class loader, so it must match the legacy one
            Thread.currentThread().setContextClassLoader(editorClassLoader());
            GeneratorPlugin[] generators = {
                new XmlMetadataGenerator(outputDir.getAbsolutePath(), schema, false, false),
                new DocumentationGenerator(schema),
                new XsdGenerator(schema, strictXsdOrder),
                new WikiDocumentationGenerator(schema),
            };
            for (NamespaceMapping namespaceMapping : namespaces) {
                for (GeneratorPlugin generator : generators) {
                    generator.setLog(this);
                    generator.generate(namespaceMapping);
                }
                if (generateSpringHandlersFile) {
                    log("Generating Spring handler mapping: " + SpringMetadataWriter.writeHandlers(outputDir, namespaceMapping.getNamespace()));
                }
                if (generateSpringSchemasFile) {
                    log("Generating Spring schema mapping: " + SpringMetadataWriter.writeSchemas(outputDir, namespaceMapping.getNamespace(), schema));
                }
            }

            if (schemaAsArtifact) {
                projectHelper.attachArtifact(project, "xsd", null, schema);
                projectHelper.attachArtifact(project, "html", "schema", new File(schema.getAbsolutePath() + ".html"));
            }
            var resource = new Resource();
            resource.setDirectory(outputDir.toString());
            project.addResource(resource);
        } catch (MojoExecutionException e) {
            throw e;
        } catch (Exception e) {
            throw new MojoExecutionException("Failed to generate the xbean schema: " + e.getMessage(), e);
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    private List<File> sourceDirectories() {
        var directories = new ArrayList<File>();
        directories.add(srcDir);
        if (includes != null) {
            for (String include : includes) {
                directories.add(new File(include));
            }
        }
        return directories;
    }

    private Set<String> excludedClassNames() {
        var names = new LinkedHashSet<String>();
        if (excludedClasses != null) {
            for (String name : excludedClasses.split(" *, *")) {
                if (!name.isBlank()) {
                    names.add(name.trim());
                }
            }
        }
        return names;
    }

    /** what javac resolves types against: the project's compile classpath plus any extra includes */
    private List<File> javacClasspath() {
        var classpath = new ArrayList<File>();
        classpath.add(new File(project.getBuild().getOutputDirectory()));
        for (Artifact artifact : project.getArtifacts()) {
            if (artifact.getFile() != null) {
                classpath.add(artifact.getFile());
            }
        }
        if (classPathIncludes != null) {
            for (String include : classPathIncludes) {
                classpath.add(new File(include));
            }
        }
        return classpath;
    }

    /**
     * The loader the generators use to look for property editors. Kept the same
     * as the legacy plugin: this project's classes and the plugin's own class
     * path, without the project's dependencies, because whether a type has an
     * editor decides whether it becomes an attribute or a nested element.
     */
    private URLClassLoader editorClassLoader() throws MojoExecutionException {
        try {
            var urls = new LinkedHashSet<URL>();
            urls.add(new File(project.getBuild().getOutputDirectory()).toURI().toURL());
            urls.add(new File(project.getBuild().getTestOutputDirectory()).toURI().toURL());
            if (classPathIncludes != null) {
                for (String include : classPathIncludes) {
                    urls.add(new File(include).toURI().toURL());
                }
            }
            return new URLClassLoader(urls.toArray(new URL[0]), getClass().getClassLoader());
        } catch (MalformedURLException e) {
            throw new MojoExecutionException("Error during setting up classpath", e);
        }
    }

    @Override
    public void log(String message) {
        getLog().info(message);
    }

    @Override
    public void log(String message, int level) {
        getLog().info(message);
    }
}
