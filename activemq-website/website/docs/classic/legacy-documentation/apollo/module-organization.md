---
title: "Apollo Module Organization"
slug: /module-organization
---

Module Organization



Module Organization
===================

Apollo is composed of many loosely coupled jar files. This helps maintain a clear separations of concerns between the modules and allows end users to end up with smaller footprint configurations if they don't need all the optional features of the project.

Dependency Diagram
------------------

<!-- Image not available: module-deps-graph.png -->

Updating
--------

You can regenerate the above graph by running the following commands at the project root:
```
mvn -P graph graph:reactor -Dhide-scope=test -Dhide-transitive=true -Dhide-external=true -Dgraph.label= -Dhide-version=true -Dhide-group-id=true -Dhide-type=true
cp target/reactor-graph.png apollo-website/src/images/module-deps-graph.png
```