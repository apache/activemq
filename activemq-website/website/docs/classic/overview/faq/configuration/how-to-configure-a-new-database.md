---
title: "How to configure a new database"
slug: /how-to-configure-a-new-database
---

ActiveMQ explicity supports Derby, Axion, HSQL, Oracle, and SQLServer. Below are the steps on how to configure a new database.

1. Modify activemq.xml found in the directory "activemq_home/conf" by editing or adding a JDBC DataSource Configuration, e.g.:

    ```
    <bean id="mysql-ds" class="org.apache.commons.dbcp.BasicDataSource" destroy-method="close">
      <property name="driverClassName" value="com.mysql.jdbc.Driver"/>
      <property name="url" value="jdbc:mysql://localhost/activemq"/>
      <property name="username" value="activemq"/>
      <property name="password" value="activemq"/>
      <property name="poolPreparedStatements" value="true"/>
    </bean>
    ```
    **For AMQ 3.x**
    ```
    <bean id="mssql-ds" class="org.apache.commons.dbcp.BasicDataSource" destroy-method="close">
        <property name="driverClassName">
            <value>com.microsoft.jdbc.sqlserver.SQLServerDriver</value>
        </property>
        <property name="url">
            <value>jdbc:microsoft:sqlserver://localhost:1433;DatabaseName=activedb</value>
        </property>
        <property name="username">
            <value>sa</value>
        </property>
        <property name="password">
            <value></value>
        </property>
        <property name="poolPreparedStatements">
            <value>true</value>
        </property>
    </bean>
    ```
2. Set the datasource reference to use the new jdbc configuration e.g `<jdbcPersistence dataSourceRef="mssql-ds"/>`

3. Place the jdbc driver in the directory "activemq_home/lib/optional".

