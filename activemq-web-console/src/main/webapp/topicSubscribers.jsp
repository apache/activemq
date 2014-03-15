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
<c:set var="pageTitle" value="Subscribers for Topic ${requestContext.topicSubscriberQuery.JMSDestination}"/>

<%@include file="decorators/head.jsp" %>
</head>
<body>

<%@include file="decorators/header.jsp" %>



<h2>Active Subscribers for <c:out value="${requestContext.topicSubscriberQuery.JMSDestination}" /></h2>

<table id="producers" class="sortable autostripe">
<thead>
<tr>
	<th>
		<span>Client ID</span>
		<br/>
		<span>Connection ID</span>
	</th>
	<th>SessionId</th>
	<th>SubscriptionId</th>
	<th>Selector</th>
	<th>Active</th>
	<th>Network</th>
	<th>Pending Queue Size</th>
	<th>Inflight</th>
	<th>Enqueued</th>
	<th>Dequeued</th>
	<th>Prefetch</th>
	<th>Subscription Name</th>
</tr>
</thead>
<tbody>
<c:forEach items="${requestContext.topicSubscriberQuery.subscribers}" var="row">
<tr>
	<td>
        <a href="<c:url value="connection.jsp?connectionID=${row.clientId}"/>"><c:out value="${row.clientId}" /></a><br/>
        <br>
        <c:out value="${row.connectionId}" />
    </td>
	<td><c:out value="${row.sessionId}" /></td>
	<td><c:out value="${row.subscriptionId}" /></td>
	<td><c:out value="${row.selector}" /></td>
	<td><c:out value="${row.active}" /></td>
	<td><c:out value="${row.network}" /></td>
	<td><c:out value="${row.pendingQueueSize}" /></td>
	<td><c:out value="${row.messageCountAwaitingAcknowledge}" /></td>
	<td><c:out value="${row.enqueueCounter}" /></td>
	<td><c:out value="${row.dequeueCounter}" /></td>
	<td><c:out value="${row.prefetchSize}" /></td>
	<td><c:out value="${row.subscriptionName}" /></td>
</tr>
</c:forEach>
</tbody>
</table>
<%@include file="decorators/footer.jsp" %>

</body>
</html>
