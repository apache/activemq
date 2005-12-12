package org.apache.jmeter.visualizers;

import org.apache.jmeter.visualizers.gui.AbstractVisualizer;
import org.apache.jmeter.samplers.Clearable;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.util.JMeterUtils;
import org.apache.jorphan.gui.ObjectTableModel;
import org.apache.jorphan.gui.layout.VerticalLayout;
import org.apache.jorphan.reflect.Functor;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.log.Logger;

import org.activemq.sampler.ConsumerSysTest;
import org.activemq.sampler.ProducerSysTest;

import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.JScrollPane;
import javax.swing.JPanel;
import javax.swing.BorderFactory;
import javax.swing.JLabel;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.border.Border;
import javax.swing.border.EmptyBorder;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.FlowLayout;
import java.util.*;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentHashMap;

/**
 * A tableVisualizer that can display Producer System Test Output.
 */
public class ConsumerSysTableVisualizer extends AbstractVisualizer implements Clearable {

    private static Logger log = LoggingManager.getLoggerForClass();
    public static boolean msgNotOrdered = false;

    private final String[] COLUMNS = new String[]{
            JMeterUtils.getResString("table_visualizer_sample_consumerid"),
            JMeterUtils.getResString("table_visualizer_sample_consumerseq_number"),
            JMeterUtils.getResString("table_visualizer_sample_prodname"),
            JMeterUtils.getResString("table_visualizer_sample_producerseq_number"),
            JMeterUtils.getResString("table_visualizer_sample_message")};

    private ObjectTableModel model = null;

    private JTable table = null;
    private JScrollPane tableScrollPanel = null;
    private JTextField messageField = null;

    /**
     *  Constructor for the TableVisualizer object.
     */
    public ConsumerSysTableVisualizer() {
        super();

        model = new ObjectTableModel(COLUMNS,
                                     new Functor[]{new Functor("getConsumerID"),
                                                   new Functor("getConsumerSeq"),
                                                   new Functor("getProdName"),
                                                   new Functor("getProducerSeq"),
                                                   new Functor("getMsgBody")},
                                     new Functor[]{null, null, null, null, null},
                                     new Class[]{String.class, Integer.class, String.class, Integer.class, String.class});

        init();
    }

    /**
     * @return Label key to get from label resource
     */
    public String getLabelResource() {

        return "view_cons_sys_results_in_table";
    }

    /**
     * @param res SampleResult from the JMeter Sampler
     */
    public void add(SampleResult res) {
        Thread timer = new Thread() {
            public void run() {
                timerLoop();
            }
        };

        timer.start();
    }

    /**
     * clear/resets the field.
     */
    public synchronized void clear() {
        log.debug("Clear called", new Exception("Debug"));
        model.clearData();
        repaint();
    }

    /**
     * Initialize the User Interface
     */
    private void init() {
        this.setLayout(new BorderLayout());

        // Main Panel
        JPanel mainPanel = new JPanel();
        Border margin = new EmptyBorder(10, 10, 5, 10);

        mainPanel.setBorder(margin);
        mainPanel.setLayout(new VerticalLayout(5, VerticalLayout.LEFT));

        // Name
        mainPanel.add(makeTitlePanel());

        // Set up the table itself
        table = new JTable(model);

        // table.getTableHeader().setReorderingAllowed(false);
        tableScrollPanel = new JScrollPane(table);
        tableScrollPanel.setViewportBorder(BorderFactory.createEmptyBorder(2, 2, 2, 2));

        // Set up footer of table which displays the messages
        JPanel messagePanel = new JPanel();
        JLabel messageLabel =
                 new JLabel(JMeterUtils.getResString("graph_results_message"));
        messageLabel.setForeground(Color.black);

        messageField = new JTextField(50);
        messageField.setBorder(BorderFactory.createEmptyBorder(0, 0, 0, 0));
        messageField.setEditable(false);
        messageField.setForeground(Color.black);
        messageField.setBackground(getBackground());
        messagePanel.add(messageLabel);
        messagePanel.add(messageField);

        // Set up info Panel table
        JPanel tableMsgPanel = new JPanel();
        tableMsgPanel.setLayout(new FlowLayout());
        tableMsgPanel.setBorder(BorderFactory.createEmptyBorder(0, 0, 0, 0));
        tableMsgPanel.add(messagePanel);

        // Set up the table with footer
        JPanel tablePanel = new JPanel();
        tablePanel.setLayout(new BorderLayout());
        tablePanel.add(tableScrollPanel, BorderLayout.CENTER);
        tablePanel.add(tableMsgPanel, BorderLayout.SOUTH);

        // Add the main panel and the graph
        this.add(mainPanel, BorderLayout.NORTH);
        this.add(tablePanel, BorderLayout.CENTER);
    }

    /**
     *  Gets the number of processed messages.
     */
    protected synchronized void timerLoop() {

        Map ProducerTextMap = new HashMap();
        Map currentProducerMap = null;
        String ProducerName = null;
        String MsgBody = null;
        String ConsumerName = null;
        String ProdSequenceNo = null;
        String mapKey = null;
        int expectedNoOfMessages = ConsumerSysTest.noOfMessages;
        int consumerCount = ConsumerSysTest.ConsumerCount;
        boolean dowhile = true;
        Map consumerMap = new ConcurrentHashMap();
        Map prodNameMap = new TreeMap();
        Map prodMsgMap = new TreeMap();

        while (dowhile) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            ConsumerSysTest consumer = new ConsumerSysTest();
            currentProducerMap = Collections.synchronizedMap(consumer.resetProducerMap());
//System.out.println("CURR MAP = " + currentProducerMap);            
//            ConsumerSysTest.ProducerMap.clear();

            if (currentProducerMap.size() == 0) {
                dowhile = false;
            }

            // Put the map values to another map for parsing.
            for (int i = 1; i <= currentProducerMap.size(); i++) {
                String ProdMsg = (String) currentProducerMap.get(String.valueOf(i));
//System.out.println("");
                ProducerName = ProdMsg.substring(0, ProdMsg.indexOf("#"));
                MsgBody = ProdMsg.substring(ProdMsg.indexOf("#")+1,  ProdMsg.indexOf("#", ProdMsg.indexOf("#")+1));
                ProdSequenceNo = ProdMsg.substring(ProdMsg.indexOf("#", ProdMsg.indexOf("#", ProdMsg.indexOf("#")+1)) + 1, ProdMsg.lastIndexOf("#"));
                ConsumerName = ProdMsg.substring(ProdMsg.lastIndexOf("#") +1, ProdMsg.length());

                if (ConsumerSysTest.destination) {
                    mapKey = ConsumerName + ProducerName;
                } else {
                    mapKey = ProducerName;
                }

                if (ProducerTextMap.containsKey(mapKey)) {
                    // Increment the counter value
                    Integer value = (Integer) ProducerTextMap.get(mapKey);

                    ProducerTextMap.put(mapKey, new Integer(value.intValue()+1));
                } else {
                    // Put the Producer Name in the map
                    ProducerTextMap.put(mapKey, new Integer(1));
                }

                Integer ConsumerSeqID = (Integer) ProducerTextMap.get(mapKey);
                Integer ProducerSeqID = Integer.valueOf(ProdSequenceNo);

                if (ConsumerSysTest.destination) {
                    // Check for duplicate message.
                    if (ConsumerSeqID.intValue() > expectedNoOfMessages) {
                        messageField.setText(JMeterUtils.getResString("duplicate_message"));
                    } else if (MsgBody.equals(ProducerSysTest.LAST_MESSAGE)) {
                        // Check for message order.
                        if (ConsumerSeqID.intValue() != expectedNoOfMessages) {
                            messageField.setText(JMeterUtils.getResString("not_in_order_message"));
                        } else if (currentProducerMap.size() == i) {
                            if (messageField.getText().length() == 0) {
                                messageField.setText(JMeterUtils.getResString("system_test_pass"));
                            }
                        }
                    }
                } else {
                    //Create map for each consumer
                    for (int j = 0 ; j < consumerCount ; j++) {
                        if (!consumerMap.containsKey(new String(ConsumerName))) {
                            consumerMap.put(new String(ConsumerName), new LinkedHashMap());
                        }
                    }

                    //create Producer Name Map
                    if (!prodNameMap.containsKey(ProducerName)) {
                        prodNameMap.put(ProducerName, (null));
                    }

                    //Get the current size of consumer
                    int seqVal = 0;
                    Object[] cObj = consumerMap.keySet().toArray();
                    for (int k = 0; k < cObj.length; k++) {
                        String cMapKey = (String)cObj[k];
                        Map cMapVal = (Map)consumerMap.get(cObj[k]);
                        if (cMapKey.equals(ConsumerName)) {
                            seqVal = cMapVal.size();
                            break;
                        }
                    }

                    //Put object to its designated consumer map
                    Object[] consumerObj = consumerMap.keySet().toArray();
                    for (int j = 0; j < consumerObj.length; j++) {
                        String cMapKey = (String)consumerObj[j];
                        Map cMapVal = (LinkedHashMap)consumerMap.get(consumerObj[j]);
                        if (cMapKey.equals(ConsumerName)) {
                            cMapVal.put(new Integer(seqVal), (ProducerName + "/" + ProducerSeqID));
                        }
                    }
                }

                // Add data to table row
                if (ConsumerSysTest.destination) {
                    SystemTestMsgSample msgSample = new SystemTestMsgSample(ConsumerName, ProducerName, MsgBody, ProducerSeqID, ConsumerSeqID);
                    model.addRow(msgSample);
                } else {
                    String msgKey = ConsumerName + "#" + ProducerName + "#" + String.valueOf(ProducerSeqID);
                    String msgVal = String.valueOf(ConsumerSeqID) + "#" + MsgBody;
                    if (!prodMsgMap.containsKey(msgKey)) {
                        prodMsgMap.put((msgKey), (msgVal));
                    }
                }
            }
        }
        if (!ConsumerSysTest.destination) {
            //Validate message sequence
            validateMsg(prodNameMap, consumerMap);
            //Populate msg sample
            populateMsgSample(prodMsgMap);
            if (msgNotOrdered) {
                messageField.setText(JMeterUtils.getResString("not_in_order_message"));
            } else {
                messageField.setText(JMeterUtils.getResString("system_test_pass"));
            }
        }
    }

    private boolean validateMsg(Map prodNameMap, Map cMap) {
        Object[]  cObj = cMap.keySet().toArray();
        for (int j = 0; j < cObj.length; j++) {
            Map childMap = (Map)cMap.get(cObj[j]);

            Object[]  nameObj = prodNameMap.keySet().toArray();
            for (int i = 0; i < nameObj.length; i++) {
                String prodName = (String)nameObj[i];
                String tempProdHolder = null;
                String tempProdIDHolder = null;

                Object[] childObj = childMap.keySet().toArray();
                for (int k = 0; k < childObj.length; k++) {
                    Integer childMapKey = (Integer)childObj[k];
                    String childMapVal = (String)childMap.get(childObj[k]);
                    String prodVal = childMapVal.substring(0, childMapVal.indexOf("/"));
                    String prodIDVal = childMapVal.substring(childMapVal.indexOf("/")+1, childMapVal.length());

                    if (prodVal.equals(prodName)) {
                        if (tempProdHolder == null) {
                            tempProdHolder = prodVal;
                            tempProdIDHolder = prodIDVal;
                            continue;
                        }
                        if (Integer.parseInt(prodIDVal) > Integer.parseInt(tempProdIDHolder)) {
                            tempProdHolder = prodVal;
                            tempProdIDHolder = prodIDVal;
                        } else {
                            msgNotOrdered = true;
                            break;
                        }
                    } else {
                        continue;
                    }
                }
            }
        }
        return msgNotOrdered;
    }

    private void populateMsgSample(Map msgMap) {
        Object[] msgObj = msgMap.keySet().toArray();
        for(int i = 0; i < msgObj.length; i++) {
            String mapKey = (String)msgObj[i];
            String mapVal = (String)msgMap.get(msgObj[i]);

            String ConsumerName = mapKey.substring(0, mapKey.indexOf("#"));
            String ProducerName = mapKey.substring(mapKey.indexOf("#")+1, mapKey.indexOf("#", mapKey.lastIndexOf("#")));
            String ProdSequenceNo = mapKey.substring(mapKey.lastIndexOf("#")+1, mapKey.length());;
            String MsgKey = mapVal.substring(0, mapVal.indexOf("#"));
            String MsgBody = mapVal.substring(mapVal.indexOf("#")+1, mapVal.length());

            Integer ConsumerSeqID = Integer.valueOf(MsgKey);
            Integer ProducerSeqID = Integer.valueOf(ProdSequenceNo);

            SystemTestMsgSample msgSample = new SystemTestMsgSample(ConsumerName, ProducerName, MsgBody, ProducerSeqID, ConsumerSeqID);
            model.addRow(msgSample);
        }
    }

}