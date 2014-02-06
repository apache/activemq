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
<title>Browse <form:short text="${requestContext.queueBrowser.JMSDestination}"/></title>
<script type="text/javascript" src="http://ajax.googleapis.com/ajax/libs/jquery/1.9.0/jquery.min.js"></script>
<script type="text/javascript" src="js/jmsQueueBrowser.js"></script>
<!-- BOOTSTRAP CSS LIBRARY -->
<link rel="stylesheet" href="//netdna.bootstrapcdn.com/bootstrap/3.1.0/css/bootstrap.min.css">
<link rel="stylesheet" href="//netdna.bootstrapcdn.com/bootstrap/3.1.0/css/bootstrap-theme.min.css">
<script src="//netdna.bootstrapcdn.com/bootstrap/3.1.0/js/bootstrap.min.js"></script>
<!-- TODO: REMOVE @import url('/admin/styles/sorttable.css'); -->
<!-- GOOGLE FONTS -->
<link href='http://fonts.googleapis.com/css?family=Open+Sans+Condensed:300' rel='stylesheet' type='text/css'>
<style type="text/css">
    body {
        font-family: 'Open Sans Condensed', sans-serif;
    }
</style>




<script type="text/javascript">
    (function(){

   $(".progress").hide();


           var jmx = new JMXJSON(null,null,function(data){
           try{
                   if (!!QueryStringHelper.toJSON().page && !!QueryStringHelper.toJSON().JMSDestination){
                       $(".progress").show();
                       $(".progress-bar").css({"width":"50%"});

                       var e = Number(QueryStringHelper.toJSON().page);
                       var last = e * 100;
                       var start = last - 100;
                       var total=data.value.TotalMessageCount;

                         //Exception cases:
                         if( total == 0 ){
                                throw "Error: No messages in the queue";
                         } else if ( start >= total &&  last > total ){
                                throw "Error: No messages on this page ";
                         } else if ( start < total && last > total ) {
                                start = ( last - total ) + start ;
                         }

                       $(".bs-callout small").html(start+" of "+last +" messages of "+ total);

                       var z = new JMSQueueBrowser(QueryStringHelper.toJSON().JMSDestination, Number(QueryStringHelper.toJSON().page), data.value.TotalMessageCount, function (data) {
                           z.empty();
                           z.setupNav();
                           $(".progress").hide();
                           return data;
                       });
                       return z;
                   }else {
                        throw "End of pages.. can not select next";
                   }
                } catch(err){
                   $(".progress").hide();
                   $("#page").hide();

                    $(".bs-callout small").html("");
                    showError("<h1>"+err+"</h1>");
                }
               console.log(data.value.TotalMessageCount);
           });

    })();
    </script>
</head>
<body>

<!-- Start Of Panel -->
<div class="panel" style="width:20%;">

<blockquote class="bs-callout"><h3>JMS Messages in Queue: <form:tooltip text="${requestContext.queueBrowser.JMSDestination}"/>
<small>1-100 of 10,000 messages</small></h3></blockquote>
<div id="pageIndicator"></div>


</div>
<!-- End Of Panel -->

<table id="messages" class="table table-striped" styles="width:20%;">
    <div class="progress" style="width: 400px;">
    <div class="progress-bar progress-bar-danger" role="progressbar" aria-valuenow="60" aria-valuemin="0" aria-valuemax="100" style="width:80%">
        <span class="sr-only">80% Complete</span>
    </div>
</div>

</table>
<!-- PAGINATION -->

<!-- PAGINATION -->
<div id="page">
<table align="center"><tr>
    <div class="pagination pagination-lg">
    <ul>
    <li><a href="">&laquo</a></li>
        <li><a href="">1</a></li>
        <li><a href="">2</a></li>
        <li><a href="">3</a></li>
        <li><a href="">4</a></li>
        <li><a href="">5</a></li>
        <li><a href="">6</a></li>
        <li><a href="">7</a></li>
        <li><a href="">8</a></li>
        <li><a href="">9</a></li>
        <li><a href="">10</a></li>
        <li><a href="">&raquo;</a></li>
    </ul>
    </div>
</tr></table></div>
<!-- PAGINATION -->
<div>
<a href="queueConsumers.jsp?JMSDestination=<c:out value="${requestContext.queueBrowser.JMSDestination}"/>">View Consumers</a>
</div>
</body>
</html>
	
