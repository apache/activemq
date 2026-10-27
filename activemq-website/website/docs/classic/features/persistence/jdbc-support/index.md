---
title: "JDBC Support"
description: "Use JDBC databases such as Oracle, MySQL, PostgreSQL, DB2 or Derby for ActiveMQ Classic persistence, with JDBCAdapter auto-discovery and custom SQL DDL."
slug: /jdbc-support
---


We support a range of SQL databases for message persistence such as

*   Apache Derby
*   Axion
*   DB2
*   HSQL
*   Informix
*   MaxDB
*   MySQL
*   Oracle
*   Postgresql
*   [SQLServer](sqlserver)
*   [Sybase](sybase)

as well as a number of generic JDBC providers.

### Auto-discovery of your JDBC provider

We try to auto-detect from the JDBC driver which JDBCAdapter to use via these [config files](https://git-wip-us.apache.org/repos/asf?p=activemq.git;a=tree;f=activemq-jdbc-store/src/main/resources/META-INF/services/org/apache/activemq/store/jdbc) and the return string from the JDBC driver.

If you have a JDBC database which we don't support then please let us know what JDBC driver string you're getting and [drop us a note](/contact) or raise an issue in our [support database](http://issues.apache.org/activemq/browse/AMQ) and we'll fix it pretty quickly.

If your database is not in the above list it is normally a matter of tweaking the StatementProvider to ensure that the JDBCMessageStore uses the right flavour of SQL. So normally most databases support one of these providers...

*   org.activemq.store.jdbc.adapter.BlobJDBCAdapter
*   org.activemq.store.jdbc.adapter.BytesJDBCAdapter
*   org.activemq.store.jdbc.adapter.DefaultJDBCAdapter
*   org.activemq.store.jdbc.adapter.ImageJDBCAdapter

You can explicitly specify the JDBC adaptor using its xbean identifier, inside the activemq.xml...
```
<jdbcPersistenceAdapter adapter="postgresql-jdbc-adapter"/>
```

### Customizing the SQL DDL

You can confiugure the various SQL datatypes - such as column sizes and so forth - using the statements element
```
<broker useJmx="false">

  <persistenceAdapter>
    <journaledJDBC useJournal="false">
      <statements>
        <statements stringIdDataType ="VARCHAR(128)"/>
      </statements>
    </journaledJDBC>
  </persistenceAdapter>

</broker>
```
For more info on what attributes can be set on the statements element, see the [Statements class](http://activemq.apache.org/components/classic/documentation/maven/apidocs/org/apache/activemq/store/jdbc/Statements.html). All the settable bean properties can be used as attributes of the <statements> element.

### Using MySQL

If you are using MySQL then you should set the **relaxAutoCommit** flag to be true. e.g.
```
<bean id="mysql-ds" class="org.apache.commons.dbcp.BasicDataSource" destroy-method="close">
  <property name="driverClassName" value="com.mysql.jdbc.Driver"/>
  <property name="url" value="jdbc:mysql://localhost/activemq?relaxAutoCommit=true"/>
  <property name="username" value="activemq"/>
  <property name="password" value="activemq"/>
  <property name="poolPreparedStatements" value="true"/>
</bean>
```
To see this in action see the [default configuration file](http://svn.apache.org/repos/asf/activemq/trunk/assembly/src/release/conf/activemq.xml)

### For AMQ 3.x

In 3.x you can specify the adapter as follows
```
<jdbcPersistence adapterClass="org.activemq.store.jdbc.adapter.ImageBasedJDBCAdaptor">
    ...
</jdbcPersistence>
```
