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
<c:set var="pageTitle" value="Browse ${requestContext.queueBrowser.JMSDestination}"/>

<%@include file="decorators/head.jsp" %>
</head>
<body>

<%@include file="decorators/header.jsp" %>



<h2>Browse <form:tooltip text="${requestContext.queueBrowser.JMSDestination}"/></h2>

<table id="messages" class="sortable autostripe">
<thead>
<tr>
<th>Message ID</th>
<th>Correlation ID</th>
<th>Persistence</th>
<th>Priority</th>
<th>Redelivered</th>
<th>Reply To</th>
<th>Timestamp</th>
<th>Type</th>
<th>Operations</th>
</tr>
</thead>
<tbody>
<jms:forEachMessage queueBrowser="${requestContext.queueBrowser.browser}" var="row">
<tr>
<td><a href="<c:url value="message.jsp">
                 <c:param name="id" value="${row.JMSMessageID}" />
                 <c:param name="JMSDestination" value="${requestContext.queueBrowser.JMSDestination}"/></c:url>"
    title="${row.properties}">${row.JMSMessageID}</a></td>
<td><c:out value="${row.JMSCorrelationID}"/></td>
<td><jms:persistent message="${row}"/></td>
<td><c:out value="${row.JMSPriority}"/></td>
<td><c:out value="${row.JMSRedelivered}"/></td>
<td><c:out value="${row.JMSReplyTo}"/></td>
<td><jms:formatTimestamp timestamp="${row.JMSTimestamp}"/></td>
<td><c:out value="${row.JMSType}"/></td>
<td>
    <a href="<c:url value="deleteMessage.action">
                 <c:param name="JMSDestination" value="${requestContext.queueBrowser.JMSDestination}"/>
                 <c:param name="messageId" value="${row.JMSMessageID}"/>
                 <c:param name="secret" value='${sessionScope["secret"]}'/></c:url>"
       onclick="return confirm('Are you sure you want to delete this message?')"
    >Delete</a>
	
    <c:set var="queueName" value="${requestContext.queueBrowser.JMSDestination}"/>
    <!-- <c:set var="queueName" value="${fn:replace(queueName, 'queue://', '')}" /> -->
    <c:set var="queueNameSubStr" value="${fn:substring(queueName, 0, 4)}" />
    
    <c:if test="${queueNameSubStr eq 'DLQ.' || queueNameSubStr eq 'DLT.'}">
        <c:if test="${queueNameSubStr eq 'DLQ.'}">
            <c:set var="moveToQueue" value="${fn:replace(queueName, 'DLQ.', '')}" />
        </c:if>
        <c:if test="${queueNameSubStr eq 'DLT.'}">
            <c:set var="moveToQueue" value="${fn:replace(queueName, 'DLT.', '')}" />
        </c:if>
        <a href="<c:url value="moveMessage.action">
                     <c:param name="destination" value="${moveToQueue}" />
                     <c:param name="JMSDestination" value="${requestContext.queueBrowser.JMSDestination}" />
                     <c:param name="messageId" value="${row.JMSMessageID}" />
                     <c:param name="JMSDestinationType" value="queue" />
                     <c:param name="secret" value='${sessionScope["secret"]}' />
                 </c:url>"
                 onclick="return confirm('Are you sure you want to retry this message on queue://<c:out value="${moveToQueue}"/>?')"
                 title="Move to <c:out value="${moveToQueue}" /> to attempt reprocessing"
        >Retry</a>
    </c:if>
</td>
</tr>
</jms:forEachMessage>
</tbody>
</table>

<div>
<a href="queueConsumers.jsp?JMSDestination=<c:out value="${requestContext.queueBrowser.JMSDestination}"/>">View Consumers</a>
</div>

<%@include file="decorators/footer.jsp" %>

</body>
</html>

