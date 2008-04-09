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
<c:set var="row" value="${requestContext.messageQuery.message}"/>
<title>Message ${requestContext.messageQuery.id}</title>
</head>
<body>


<c:choose>
<c:when test="${empty row}">

<div>
No message could be found for ID ${requestContext.messageQuery.JMSMessageID}
</div>

</c:when>

<c:otherwise>

<table class="layout">
	<tr>
		<td class="layout"  valign="top">
			<table id="header" class="sortable autostripe">
				<thead>
					<tr>
						<th colspan="2">
						    Headers
						</th>
					</tr>
				</thead>
				<tbody>
					<tr>
						<td class="label" title="Unique Message ID for this message">Message ID</td>
						<td>${row.JMSMessageID}</td>
					</tr>
					<tr>
						<td class="label">Destination</td>
						<td>${row.JMSDestination}</td>
					</tr>
					<tr>
						<td class="label" title="The ID used to correlate messages together in a conversation">Correlation ID</td>
						<td>${row.JMSCorrelationID}</td>
					</tr>
					<tr>
						<td class="label" title="Message Group Identifier">Group</td>
						<td>${row.groupID}</td>
					</tr>
					<tr>
						<td class="label" title="Message Group Sequence Number">Sequence</td>
						<td>${row.groupSequence}</td>
					</tr>
					<tr>
						<td class="label">Expiration</td>
						<td>${row.JMSExpiration}</td>
					</tr>
					<tr>
						<td class="label">Persistence</td>
						<td><jms:persistent message="${row}"/></td>
					</tr>
					<tr>
						<td class="label">Priority</td>
						<td>${row.JMSPriority}</td>
					</tr>
					<tr>
						<td class="label">Redelivered</td>
					    <td>${row.JMSRedelivered}</td>
					</tr>
					<tr>
						<td class="label">Reply To</td>
						<td>${row.JMSReplyTo}</td>
					</tr>
					<tr>
						<td class="label">Timestamp</td>
						<td>${row.JMSTimestamp}</td>
					</tr>
					<tr>
						<td class="label">Type</td>
						<td>${row.JMSType}</td>
					</tr>
				</tbody>
			</table>
        </td>

        <td  class="layout" valign="top">
			<table id="properties" class="sortable autostripe">
				<thead>
					<tr>
						<th colspan="2">
						    Properties
						</th>
					</tr>
				</thead>
				<tbody>
                   <form:forEachMapEntry items="${requestContext.messageQuery.propertiesMap}" var="prop">
						<tr>
							<td class="label">${prop.key}</td>
							<td>${prop.value}</td>
						</tr>
						<tr>
					</form:forEachMapEntry>
				</tbody>
			</table>
		</td>
	</tr>
	<tr>
		<td class="layout" colspan="2">
			<table id="body" width="100%">
				<thead>
					<tr>
						<th>
						    Message Details
						</th>
					</tr>
				</thead>
				<tbody>
					<tr>
						<td><c:out value="${requestContext.messageQuery.body}" escapeXml="true" /></td>
					</tr>
				</tbody>
			</table>
		</td>
	</tr>
</table>


</c:otherwise>
</c:choose>





</body>
</html>
	
