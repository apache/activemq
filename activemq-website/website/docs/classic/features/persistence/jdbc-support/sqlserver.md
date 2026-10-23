---
title: "SQLServer"
description: "ActiveMQ Classic JDBC persistence examples for Microsoft SQL Server 2000 and 2005 drivers and jTDS, plus Windows TCP registry tweaks for connection starvation."
slug: /sqlserver
---


SQL Server
----------

### SQL Server 2000 Drivers 

Here is an example of a configuration for SQLServer kindly supplied by Ning Li

<persistenceAdapter>
     <journaledJDBC journalLogFiles="5"
dataDirectory="../activemq-data" dataSource="#mssql-ds">
      <adapter><imageBasedJDBCAdaptor/></adapter>
     </journaledJDBC>
</persistenceAdapter>


 <bean id="mssql-ds" class="org.apache.commons.dbcp.BasicDataSource"
destroy-method="close">
   <property name="driverClassName"
value="com.microsoft.jdbc.sqlserver.SQLServerDriver"/>
   <property name="url" value="jdbc:microsoft:sqlserver://HOSTNAME
\\\INSTANCE;DataBaseName=DBNAME"/>
   <property name="username" value="sa"/>
   <property name="password" value="password"/>
 </bean>

Information about the Microsoft SqlServer 2000 drivers can be found [here](http://support.microsoft.com/kb/313100/)

### SQL Server 2005 Drivers

Here is an example of the configuration for Microsoft SQL Server 2005 drivers

<persistenceAdapter>
  <journaledJDBC journalLogFiles="5" dataDirectory="../activemq-data" dataSource="#mssql-ds">
    <adapter><imageBasedJDBCAdaptor/></adapter>
  </journaledJDBC>
</persistenceAdapter>

<bean id="mssql-ds" class="org.apache.commons.dbcp.BasicDataSource" destroy-method="close">
  <property name="driverClassName" value="com.microsoft.sqlserver.jdbc.SQLServerDriver"/>
  <property name="url" value="jdbc:sqlserver://HOST:PORT;databaseName=DBNAME;user=USER;password=PASSWORD"/>
  <property name="username" value="USER"/>
  <property name="password" value="PASSWORD"/>
</bean>

Information about the Microsoft SqlServer 2005 drivers can be found [here](http://msdn.microsoft.com/data/ref/jdbc/)

### jTDS Drivers

Here's an example of the configuration for the jTDS drivers:

<bean id="mssql-ds" class="net.sourceforge.jtds.jdbcx.JtdsDataSource">
   <property name="serverName" value="SERVERNAME"/>
   <property name="portNumber" value="PORTNUMBER"/>
   <property name="databaseName" value="DATABASENAME"/>
   <property name="user" value="USER"/>
   <property name="password" value="PASSWORD"/>
 </bean>

A note about the jTDS Drivers: Using both the JtdsDataSource and the DBCP BasicDataSource seem to result in client connection starvation on Windows. The JtdsDataSource is better behaved, though, i.e. it doesn't throw an exception. There is mileage in upping the maximum number of client ports and dropping the Tcp wait delay down to 30 seconds. Use RegEdit to add the following values:

Name

Type

Key

Value

Description

TcpTimedWaitDelay

DWORD

HKEY\_LOCAL\_MACHINE\\SYSTEM\\CurrentControlSet\ Services\\TCPIP\\Parameters

30 (Decimal)

Reduces the time a connection spends in TIME_WAIT to 30 seconds

MaxUserPort

DWORD

HKEY\_LOCAL\_MACHINE\\SYSTEM\\CurrentControlSet\ Services\\TCPIP\\Parameters

32768 (Decimal)

Increases the number of ports a client application can use to 32768

