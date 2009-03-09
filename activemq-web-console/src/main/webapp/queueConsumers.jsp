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
<title>Consumers for ${requestContext.queueConsumerQuery.JMSDestination}</title>
</head>
<body>

<h2>Active Consumers for ${requestContext.queueConsumerQuery.JMSDestination}</h2>

<table id="messages" class="sortable autostripe">
<thead>
<tr>
	<th>
		<span>Client ID</span>
		<br/>
		<span>Connection ID</span>
	</th>
	<th>SessionId</th>
	<th>Selector</th>
	<th>Enqueues</th>
	<th>Dequeues</th>
	<th>Dispatched</th>
	<th>Dispatched Queue</th>
	<th>
		<span>Prefetch</span>
		<br/>
		<span>Max pending</span>
	</th>
	<th>
		<span>Exclusive</span>
		<br/>
		<span>Retroactive</span>
	</th>
</tr>
</thead>
<tbody>
<c:forEach items="${requestContext.queueConsumerQuery.consumers}" var="row">
<tr>
	<td>
		<a href="connection.jsp?connectionID=${row.clientId}">${row.clientId}</a><br/>
	    ${row.connectionId}</a>
	</td>
	<td>${row.sessionId}</td>
	<td>${row.selector}</td>
	<td>${row.enqueueCounter}</td>
	<td>${row.dequeueCounter}</td>
	<td>${row.dispachedCounter}</td>
	<td>${row.dispatchedQueueSize}</td>
	<td>
		${row.prefetchSize}<br/>
		${row.maximumPendingMessageLimit}
	</td>
	<td>
		${row.exclusive}<br/>
		${row.retroactive}
	</td>
</tr>
</c:forEach>
</tbody>
</table>

</body>
</html>
	