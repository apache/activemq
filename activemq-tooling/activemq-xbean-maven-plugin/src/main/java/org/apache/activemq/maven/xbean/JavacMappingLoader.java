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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.stream.Stream;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.StandardLocation;
import javax.tools.ToolProvider;

import org.apache.xbean.spring.generator.ElementMapping;
import org.apache.xbean.spring.generator.InvalidModelException;
import org.apache.xbean.spring.generator.MappingLoader;
import org.apache.xbean.spring.generator.NamespaceMapping;

/**
 * A {@link MappingLoader} that reads the sources with the JDK's own compiler
 * instead of QDox. javac runs in process with {@code -proc:only}, so it
 * resolves supertypes and members against the classpath but attributes no
 * method bodies; {@link XBeanModelProcessor} then reads the model.
 */
public final class JavacMappingLoader implements MappingLoader {

    private final String namespace;
    private final List<File> sourceDirectories;
    private final Set<String> excludedClasses;
    private final List<File> classpath;
    private final boolean failOnCompilerErrors;
    private final Consumer<String> info;
    private final Consumer<String> debug;

    public JavacMappingLoader(String namespace, List<File> sourceDirectories, Set<String> excludedClasses, List<File> classpath,
            boolean failOnCompilerErrors, Consumer<String> info, Consumer<String> debug) {
        this.namespace = namespace;
        this.sourceDirectories = sourceDirectories;
        this.excludedClasses = excludedClasses;
        this.classpath = classpath;
        this.failOnCompilerErrors = failOnCompilerErrors;
        this.info = info;
        this.debug = debug;
    }

    @Override
    public Set<NamespaceMapping> loadNamespaces() throws IOException {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        if (compiler == null) {
            throw new IOException("No system Java compiler available; the build needs a JDK, not a JRE");
        }
        List<File> sources = listSourceFiles();
        info.accept("Reading " + sources.size() + " source files from " + sourceDirectories.size() + " directories");

        var processor = new XBeanModelProcessor(namespace, excludedClasses, info, debug);
        var diagnostics = new DiagnosticCollector<JavaFileObject>();
        try (StandardJavaFileManager fileManager = compiler.getStandardFileManager(diagnostics, null, StandardCharsets.UTF_8)) {
            fileManager.setLocation(StandardLocation.CLASS_PATH, classpath);
            Iterable<? extends JavaFileObject> units = fileManager.getJavaFileObjectsFromFiles(sources);
            List<String> options = List.of("-proc:only", "-implicit:none", "-Xlint:none");
            JavaCompiler.CompilationTask task = compiler.getTask(null, fileManager, diagnostics, options, null, units);
            task.setProcessors(List.of(processor));
            try {
                task.call();
            } catch (RuntimeException e) {
                // javac wraps what a processor throws; surface a model problem as itself
                for (Throwable cause = e; cause != null; cause = cause.getCause()) {
                    if (cause instanceof InvalidModelException invalid) {
                        throw invalid;
                    }
                }
                throw e;
            }
        }

        int errors = 0;
        for (Diagnostic<? extends JavaFileObject> diagnostic : diagnostics.getDiagnostics()) {
            if (diagnostic.getKind() == Diagnostic.Kind.ERROR) {
                errors++;
                debug.accept("javac: " + diagnostic.getMessage(null));
            }
        }
        if (errors > 0) {
            String message = errors + " javac errors while reading the sources; unresolved third party imports are harmless here";
            if (failOnCompilerErrors) {
                throw new IOException(message);
            }
            info.accept(message + " (run with -X to list them)");
        }
        return groupByNamespace(processor.getElements());
    }

    private List<File> listSourceFiles() throws IOException {
        var files = new ArrayList<File>();
        for (File directory : sourceDirectories) {
            if (!directory.isDirectory()) {
                info.accept("Specified source directory isn't a directory: '" + directory.getAbsolutePath() + "'.");
                continue;
            }
            try (Stream<Path> walk = Files.walk(directory.toPath())) {
                walk.filter(path -> path.toString().endsWith(".java"))
                        .filter(Files::isRegularFile)
                        .sorted()
                        .forEach(path -> files.add(path.toFile()));
            }
        }
        return files;
    }

    /** same grouping as xbean's QdoxMappingLoader: one NamespaceMapping per namespace, with its root element if any */
    private Set<NamespaceMapping> groupByNamespace(List<ElementMapping> elements) {
        var elementsByNamespace = new HashMap<String, Set<ElementMapping>>();
        var namespaceRoots = new HashMap<String, ElementMapping>();
        for (ElementMapping element : elements) {
            elementsByNamespace.computeIfAbsent(element.getNamespace(), k -> new HashSet<>()).add(element);
            if (element.isRootElement()) {
                if (namespaceRoots.containsKey(element.getNamespace())) {
                    info.accept("Multiple root elements found for namespace " + element.getNamespace());
                }
                namespaceRoots.put(element.getNamespace(), element);
            }
        }
        var namespaces = new TreeSet<NamespaceMapping>();
        for (Map.Entry<String, Set<ElementMapping>> entry : elementsByNamespace.entrySet()) {
            namespaces.add(new NamespaceMapping(entry.getKey(), entry.getValue(), namespaceRoots.get(entry.getKey())));
        }
        return Collections.unmodifiableSet(namespaces);
    }
}
