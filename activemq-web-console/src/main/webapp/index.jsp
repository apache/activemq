<%--
    Licensed to the Apache Software Foundation (ASF) under one or more
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership.
    The ASF licenses this file to You under the Apache License, Version 2.0
    (the "License"); you may not use this file except in compliance with
    the License.  You may obtain a copy of the License at
   
    http://www.apache.org/licenses/LICENSE-2.0
   
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
--%>
<%-- Workaround for https://ops4j1.jira.com/browse/PAXWEB-1070 --%>
<%@include file="WEB-INF/jspf/headertags.jspf" %>
<html>
<head>
<c:set var="pageTitle" value="ActiveMQ Console"/>

<%@include file="decorators/head.jsp" %>
</head>
<body>

<%@include file="decorators/header.jsp" %>

<h2>Welcome!</h2>

<p>
Welcome to the Apache ActiveMQ Console of <b><c:out value="${requestContext.brokerQuery.brokerName}" /></b> (<c:out value="${requestContext.brokerQuery.brokerAdmin.brokerId}" />)
</p>

<p>
You can find more information about Apache ActiveMQ on the <a href="<c:url value="http://activemq.apache.org/" />">Apache ActiveMQ Site</a>
</p>

<h2>Broker</h2>


<table>
    <tr>
        <td>Name</td>
        <td><b><c:out value="${requestContext.brokerQuery.brokerAdmin.brokerName}" /></b></td>
    </tr>
    <tr>
        <td>Version</td>
        <td><b><c:out value="${requestContext.brokerQuery.brokerAdmin.brokerVersion}" /></b></td>
    </tr>
    <tr>
        <td>ID</td>
        <td><b><c:out value="${requestContext.brokerQuery.brokerAdmin.brokerId}" /></b></td>
    </tr>
    <tr>
        <td>Uptime</td>
        <td><b><c:out value="${requestContext.brokerQuery.brokerAdmin.uptime}" /></b></td>
    </tr>
    <tr>
        <td>Store percent used</td>
        <td><b><c:out value="${requestContext.brokerQuery.brokerAdmin.storePercentUsage}" /></b></td>
    </tr>
    <tr>
        <td>Memory percent used</td>
        <td><b><c:out value="${requestContext.brokerQuery.brokerAdmin.memoryPercentUsage}" /></b></td>
    </tr>
    <tr>
        <td>Temp percent used</td>
        <td><b><c:out value="${requestContext.brokerQuery.brokerAdmin.tempPercentUsage}" /></b></td>
    </tr>
</table>
<%@include file="decorators/footer.jsp" %>

</body>
</html>
	
