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
<c:set var="row" value="${requestContext.connectionQuery.connection}"/>
<title>Connection ${requestContext.connectionQuery.connectionID}</title>
</head>
<body>


<c:choose>
<c:when test="${empty row}">

<div>
No connection could be found for ID ${requestContext.connectionQuery.connectionID}
</div>

</c:when>

<c:otherwise>

<h2>Connection ${requestContext.connectionQuery.connectionID}</h2>

<table id="header" class="sortable autostripe">
	<tbody>
		<tr>
			<td class="label" title="Unique ID for this connection">Connection ID</td>
			<td>${requestContext.connectionQuery.connectionID}</td>
		</tr>
		<tr>
			<td class="label" tite="Hostname and port of the connected party">Remote Address</td>
			<td>${row.remoteAddress}</td>
		</tr>
		<tr>
			<td class="label">Active</td>
			<td>${row.active}</td>
		</tr>
		<tr>
			<td class="label">Connected</td>
			<td>${row.connected}</td>
		</tr>
		<tr>
			<td class="label">Blocked</td>
			<td>${row.blocked}</td>
		</tr>
		<tr>
			<td class="label">Slow</td>
			<td>${row.slow}</td>
		</tr>
		<tr>
			<td class="label">Enqueue Count</td>
			<td>${row.enqueueCount}</td>
		</tr>
		<tr>
			<td class="label">Dequeue Count</td>
			<td>${row.dequeueCount}</td>
		</tr>
		<tr>
			<td class="label">Dispatch Queue Size</td>
		    <td>${row.dispatchQueueSize}</td>
		</tr>
	</tbody>
</table>



<h3>Consumers</h3>

<table id="messages" class="sortable autostripe">
<thead>
<tr>
	<th>Destination</th>
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
<c:forEach items="${requestContext.connectionQuery.consumers}" var="consumer">
<tr>
	<td>
		<c:choose>
			<c:when test="${consumer.destinationQueue}">
				Queue <a href="browse.jsp?JMSDestination=${consumer.destinationName}">${consumer.destinationName}</a>
			</c:when>
			<c:when test="${consumer.destinationTopic}">
				Topic <a href="send.jsp?JMSDestination=${consumer.destinationName}">${consumer.destinationName}</a>
			</c:when>
			<c:otherwise>
				${consumer.destinationName}
			</c:otherwise>
		</c:choose>
	</td>
	<td>${consumer.sessionId}</td>
	<td>${consumer.selector}</td>
	<td>${consumer.enqueueCounter}</td>
	<td>${consumer.dequeueCounter}</td>
	<td>${consumer.dispachedCounter}</td>
	<td>${consumer.dispatchedQueueSize}</td>
	<td>
		${consumer.prefetchSize}<br/>
		${consumer.maximumPendingMessageLimit}
	</td>
	<td>
		${consumer.exclusive}<br/>
		${consumer.retroactive}
	</td>
</tr>
</c:forEach>
</tbody>
</table>


</c:otherwise>
</c:choose>





</body>
</html>
	