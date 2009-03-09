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
<title>Connections</title>
</head>
<body>

<h2>Connections</h2>

<c:forEach items="${requestContext.brokerQuery.connectors}" var="connectorName">
<h3>Connector ${connectorName}</h3>

<table id="connections" class="sortable autostripe">
<thead>
<tr>
	<th>Name</th>
	<th>Remote Address</th>
	<th>Enqueue Count</th>
	<th>Dequeue Count</th>
	<th>Dispatch Queue Size</th>
	<th>Active</th>
	<th>Slow</th>
</tr>
</thead>
<tbody>
<jms:forEachConnection broker="${requestContext.brokerQuery}" connectorName="${connectorName}"
	connection="con" connectionName="conName">
<tr>
	<td><a href="connection.jsp?connectionID=${conName}">${conName}</a></td>
	<td>${con.remoteAddress}</td>
	<td>${con.enqueueCount}</td>
	<td>${con.dequeueCount}</td>
	<td>${con.dispatchQueueSize}</td>
	<td>${con.active}</td>
	<td>${con.slow}</td>
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
	<th>Network TTL</th>
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
	<td>${nc.name}</td>
	<td>${nc.networkTTL}</td>
	<td>${nc.dynamicOnly}</td>
	<td>${nc.conduitSubscriptions}</td>
	<td>${nc.bridgeTempDestinations}</td>
	<td>${nc.decreaseNetworkConsumerPriority}</td>
	<td>${nc.dispatchAsync}</td>
</tr>
</c:forEach>
</tbody>
</table>
</div>

</body>
</html>
	
