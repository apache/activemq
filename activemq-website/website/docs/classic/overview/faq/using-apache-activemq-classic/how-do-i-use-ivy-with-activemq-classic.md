---
title: "How do I use Ivy with ActiveMQ"
slug: /how-do-i-use-ivy-with-activemq-classic
---

```
<ivyconf>
        <!--loads properties file as ivy variables, 0..n-->
        <properties file="${ivy.conf.dir}/ivyconf-file.properties" />
        <!--configures ivy with some defaults, 0..1-->
        <conf defaultResolver="localChain" checkUpToDate="false" />

        <!--typedef:defines new types in ivy-->
        <!--latest-strategies: defines latest strategies-->
        <!--conflict-managers: defines conflicts managers-->

        <!--defines dependency resolvers-->
        <resolvers>
             <chain name="localChain" returnFirst="false">
                 <filesystem name="internal" latest="latest-revision">
                        <ivy pattern="${repository.dir}/[organisation]/[module]/[type]s/ivy-[revision].xml" />
                        <artifact pattern="${repository.dir}/[organisation]/[module]/[type]s/[artifact]-[revision].[ext]" />
                </filesystem>
                <ivyrep name="ivyrep"/>
             </chain>
             <ibiblio name="ibiblio" pattern="[organisation]/jars/[module]-[revision].[ext]"/>
        </resolvers>

        <!--defines rules between modules and dependency resolvers-->
        <modules>
                <module organisation="verticon" name=".*" resolver="internal" />
                <module organisation="mandarax" name=".*" resolver="internal" />
                <module organisation="geronimo-spec" name=".*" resolver="ibiblio" />
                <module organisation="tomcat" name=".*" resolver="ibiblio" />
        </modules>

</ivyconf>
```
