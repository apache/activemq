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
<c:set var="pageTitle" value="Connection ${requestContext.connectionQuery.connectionID}"/>

<%@include file="decorators/head.jsp" %>
</head>
<body>

<%@include file="decorators/header.jsp" %>



<c:choose>
<c:when test="${empty row}">

<div>
No connection could be found for ID <c:out value="${requestContext.connectionQuery.connectionID}" />
</div>

</c:when>

<c:otherwise>

<h2>Connection <c:out value="${requestContext.connectionQuery.connectionID}" /></h2>

<table id="header" class="sortable autostripe">
	<tbody>
		<tr>
			<td class="label" title="Unique ID for this connection">Connection ID</td>
			<td><c:out value="${requestContext.connectionQuery.connectionID}" /></td>
		</tr>
		<tr>
			<td class="label" tite="Hostname and port of the connected party">Remote Address</td>
			<td><c:out value="${row.remoteAddress}" /></td>
		</tr>
		<tr>
			<td class="label">Active</td>
			<td><c:out value="${row.active}" /></td>
		</tr>
		<tr>
			<td class="label">Connected</td>
			<td><c:out value="${row.connected}" /></td>
		</tr>
		<tr>
			<td class="label">Blocked</td>
			<td><c:out value="${row.blocked}" /></td>
		</tr>
		<tr>
			<td class="label">Slow</td>
			<td><c:out value="${row.slow}" /></td>
		</tr>
		<tr>
			<td class="label">Dispatch Queue Size</td>
		    <td><c:out value="${row.dispatchQueueSize}" /></td>
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
				Queue <a href="browse.jsp?JMSDestination=<c:out value="${consumer.destinationName}" />"><c:out value="${consumer.destinationName}" /></a>
			</c:when>
			<c:when test="${consumer.destinationTopic}">
				Topic <a href="send.jsp?JMSDestination=<c:out value="${consumer.destinationName}" />"><c:out value="${consumer.destinationName}" /></a>
			</c:when>
			<c:otherwise>
				<c:out value="${consumer.destinationName}" />
			</c:otherwise>
		</c:choose>
	</td>
	<td><c:out value="${consumer.sessionId}" /></td>
	<td><c:out value="${consumer.selector}" /></td>
	<td><c:out value="${consumer.enqueueCounter}" /></td>
	<td><c:out value="${consumer.dequeueCounter}" /></td>
	<td><c:out value="${consumer.dispatchedCounter}" /></td>
	<td><c:out value="${consumer.dispatchedQueueSize}" /></td>
	<td>
		<c:out value="${consumer.prefetchSize}" /><br/>
		<c:out value="${consumer.maximumPendingMessageLimit}" />
	</td>
	<td>
		<c:out value="${consumer.exclusive}" /><br/>
		<c:out value="${consumer.retroactive}" />
	</td>
</tr>
</c:forEach>
</tbody>
</table>


</c:otherwise>
</c:choose>



<%@include file="decorators/footer.jsp" %>


</body>
</html>
	