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
<html>
<head>
<c:set var="pageTitle" value="Connections"/>

<%@include file="decorators/head.jsp" %>
</head>
<body>

<%@include file="decorators/header.jsp" %>



<h2>Connections</h2>

<c:forEach items="${requestContext.brokerQuery.connectors}" var="connectorName">
<h3>Connector <c:out value="${connectorName}" /></h3>

<table id="connections" class="sortable autostripe">
<thead>
<tr>
	<th>Name</th>
	<th>Remote Address</th>
	<th>Active</th>
	<th>Slow</th>
</tr>
</thead>
<tbody>
<jms:forEachConnection broker="${requestContext.brokerQuery}" connectorName="${connectorName}"
	connection="con" connectionName="conName">
<tr>

	<td><a href="<c:url value='connection.jsp'><c:param name='connectionID' value='${conName}' /></c:url>"><c:out value="${conName}" /></a></td>
	<td><c:out value="${con.remoteAddress}" /></td>
	<td><c:out value="${con.active}" /></td>
	<td><c:out value="${con.slow}" /></td>
</tr>
</jms:forEachConnection>
</tbody>
</table>

</c:forEach>
<div style="margin-top: 5em">
<h2>Network Connectors</h2>

<table id="connections" class="sortable autostripe">
<thead>
<tr>
	<th>Name</th>
	<th>Message TTL</th>
	<th>Consumer TTL</th>
	<th>Dynamic Only</th>
	<th>Conduit Subscriptions</th>
	<th>Bridge Temps</th>
	<th>Decrease Priorities</th>
	<th>Dispatch Async</th>
</tr>
</thead>
<tbody>
<c:forEach items="${requestContext.brokerQuery.networkConnectors}" var="nc">
<tr>
	<td><c:out value="${nc.name}" /></td>
	<td><c:out value="${nc.messageTTL}" /></td>
	<td><c:out value="${nc.consumerTTL}" /></td>
	<td><c:out value="${nc.dynamicOnly}" /></td>
	<td><c:out value="${nc.conduitSubscriptions}" /></td>
	<td><c:out value="${nc.bridgeTempDestinations}" /></td>
	<td><c:out value="${nc.decreaseNetworkConsumerPriority}" /></td>
	<td><c:out value="${nc.dispatchAsync}" /></td>
</tr>
</c:forEach>
</tbody>
</table>
</div>
<%@include file="decorators/footer.jsp" %>

</body>
</html>
	
