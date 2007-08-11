/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.web;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Hashtable;
import java.util.Map;

/**
 * A servlet which will publish dummy market data prices
 * 
 * @version $Revision: 1.1.1.1 $
 */
public class PortfolioPublishServlet extends MessageServletSupport {

    private static final int maxDeltaPercent = 1;
    private static final Map lastPrices = new Hashtable();
    private boolean ricoStyle = true;

    
    public void init() throws ServletException {
        super.init();
        
        ricoStyle = asBoolean(getServletConfig().getInitParameter("rico"), true);
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        PrintWriter out = response.getWriter();
        String[] stocks = request.getParameterValues("stocks");
        if (stocks == null || stocks.length == 0) {
            out.println("<html><body>No <b>stocks</b> query parameter specified. Cannot publish market data</body></html>");
        }
        else {
            Integer total=(Integer)request.getSession(true).getAttribute("total");
            if (total==null)
                total=Integer.valueOf(0);
            
            
            int count = getNumberOfMessages(request);
            total=Integer.valueOf(total.intValue()+count);
            request.getSession().setAttribute("total",total);
            
            try {
                WebClient client = WebClient.getWebClient(request);
                for (int i = 0; i < count; i++) {
                    sendMessage(client, stocks);
                }
                out.print("<html><head><meta http-equiv='refresh' content='");
                String refreshRate = request.getParameter("refresh");
                if (refreshRate == null || refreshRate.length() == 0) {
                    refreshRate = "1";
                }
                out.print(refreshRate);
                out.println("'/></head>");
                out.println("<body>Published <b>" + count + "</b> of "+total+ " price messages.  Refresh = "+refreshRate+"s");
                out.println("</body></html>");

            }
            catch (JMSException e) {
                out.println("<html><body>Failed sending price messages due to <b>" + e + "</b></body></html>");
                log("Failed to send message: " + e, e);
            }
        }
    }

    protected void sendMessage(WebClient client, String[] stocks) throws JMSException {
        Session session = client.getSession();

        int idx = 0;
        while (true) {
            idx = (int) Math.round(stocks.length * Math.random());
            if (idx < stocks.length) {
                break;
            }
        }
        String stock = stocks[idx];
        Destination destination = session.createTopic("STOCKS." + stock);
        String stockText = createStockText(stock);
        log("Sending: " + stockText + " on destination: " + destination);
        Message message = session.createTextMessage(stockText);
        client.send(destination, message);
    }

    protected String createStockText(String stock) {
        Double value = (Double) lastPrices.get(stock);
        if (value == null) {
            value = new Double(Math.random() * 100);
        }

        // lets mutate the value by some percentage
        double oldPrice = value.doubleValue();
        value = new Double(mutatePrice(oldPrice));
        lastPrices.put(stock, value);
        double price = value.doubleValue();

        double offer = price * 1.001;

        String movement = (price > oldPrice) ? "up" : "down";
        return "<price stock='" + stock + "' bid='" + price + "' offer='" + offer + "' movement='" + movement + "'/>";
    }

    protected double mutatePrice(double price) {
        double percentChange = (2 * Math.random() * maxDeltaPercent) - maxDeltaPercent;

        return price * (100 + percentChange) / 100;
    }

    protected int getNumberOfMessages(HttpServletRequest request) {
        String name = request.getParameter("count");
        if (name != null) {
            return Integer.parseInt(name);
        }
        return 1;
    }
}
