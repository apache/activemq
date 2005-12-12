/**
 *
 * Copyright 2004 Protique Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **/
package org.activemq.sampler.config.gui;

import org.apache.jmeter.config.ConfigTestElement;
import org.apache.jmeter.config.gui.AbstractConfigGui;
import org.apache.jmeter.gui.util.VerticalPanel;
import org.apache.jmeter.testelement.TestElement;
import org.apache.jmeter.util.JMeterUtils;
import org.apache.jorphan.util.JOrphanUtils;
import org.activemq.sampler.Consumer;


import javax.swing.JTextField;
import javax.swing.JComboBox;
import javax.swing.JRadioButton;
import javax.swing.JPanel;
import javax.swing.JLabel;

import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

/**
 * Consumer configuration gui bean.
 */
public class ConsumerConfigGui extends AbstractConfigGui {

    //private final static String FILENAME = "filename";
    private final static String URL = "url";
    private final static String DURATION = "duration";
    private final static String RAMP_UP = "ramp_up";
    private final static String DURABLE = "durable";
    private final static String NONDURABLE = "nondurable";
    private final static String TOPIC = "topic";
    private final static String QUEUE = "queue";
    private final static String NOCONSUMER = "noconsumer";
    private final static String NOSUBJECT = "nosubject";
    private final static String CONSUMER_CONFIG_TITLE = "consumer_config_title";
    private final static String MQSERVER = "mqserver";
    private static final String ACTIVEMQ_SERVER = JMeterUtils.getResString("activemq_server");
    private static final String SONICMQ_SERVER = JMeterUtils.getResString("sonicmq_server");
    private static final String TIBCOMQ_SERVER = JMeterUtils.getResString("tibcomq_server");
    private static final String JBOSSMQ_SERVER = JMeterUtils.getResString("jbossmq_server");
    private static final String OPENJMS_SERVER = JMeterUtils.getResString("openjms_server");
    private static final String JORAM_SERVER = JMeterUtils.getResString("joram_server");
    private static final String MANTARAY_SERVER = JMeterUtils.getResString("mantaray_server");
    private static final String TRANSACTED = "transacted";
    private static final String NONTRANSACTED = "nontransacted";
    private static final String BATCHSIZE = "batchsize";

    private JTextField setURL;
    private JTextField setDuration;
    private JTextField setRampUp;
    private JTextField setNoConsumer;
    private JTextField setNoSubject;

    private JRadioButton setDurable;
    private JRadioButton setNonDurable;
    private JRadioButton setTopic;
    private JRadioButton setQueue;
    private JComboBox setMQServer;
    private JRadioButton setTransacted;
    private JRadioButton setNonTransacted;
    private JTextField setBatchSize;

    private boolean displayName = true;

    /**
     * Default constructor.
     */
    public ConsumerConfigGui() {
        this(true);
    }

    /**
     * Constructor.
     *
     * @param displayName - whether to display the name of the consumer.
     */
    public ConsumerConfigGui(boolean displayName) {
        this.displayName = displayName;
        init();
    }

    /**
     * Returns the consumer configuration title.
     *
     * @return consumer configuration title
     */
    public String getLabelResource() {
        return CONSUMER_CONFIG_TITLE;
    }

    /**
     * Configures the ConsumerConfigGui bean.
     *
     * @param element - consumer sampler properties.
     */
    public void configure(TestElement element) {
        super.configure(element);

        setURL.setText(element.getPropertyAsString(Consumer.URL));
        setDuration.setText(element.getPropertyAsString(Consumer.DURATION));
        setRampUp.setText(element.getPropertyAsString(Consumer.RAMP_UP));

        if (element.getProperty(Consumer.DURABLE) == null) {
            setDurable.setSelected(false);
            setNonDurable.setSelected(true);
        } else {
            if (element.getPropertyAsBoolean(Consumer.DURABLE)) {
                setDurable.setSelected(true);
                setNonDurable.setSelected(false);
            } else {
                setDurable.setSelected(false);
                setNonDurable.setSelected(true);
            }
        }

        if (element.getProperty(Consumer.TOPIC) == null) {
            setTopic.setSelected(true);
            setQueue.setSelected(false);
        } else {
            if (element.getPropertyAsBoolean(Consumer.TOPIC)) {
                setTopic.setSelected(true);
                setQueue.setSelected(false);
            } else {
                setTopic.setSelected(false);
                setQueue.setSelected(true);
            }
        }

        setNoConsumer.setText(element.getPropertyAsString(Consumer.NOCONSUMER));
        setNoSubject.setText(element.getPropertyAsString(Consumer.NOSUBJECT));
        setMQServer.setSelectedItem(element.getPropertyAsString(Consumer.MQSERVER));
        if (element.getProperty(Consumer.TRANSACTED) == null) {
            setTransacted.setSelected(false);
            setNonTransacted.setSelected(true);
            setBatchSize.setEnabled(false);
        } else {
            if (element.getPropertyAsBoolean(Consumer.TRANSACTED)) {
                setTransacted.setSelected(true);
                setNonTransacted.setSelected(false);
                setBatchSize.setEnabled(true);
                setBatchSize.setText(element.getPropertyAsString(Consumer.BATCHSIZE));
            } else {
                setTransacted.setSelected(false);
                setNonTransacted.setSelected(true);
                setBatchSize.setEnabled(false);
                setBatchSize.setText("");
            }
        }
    }

    /**
     * Creates a test element.
     *
     * @return element
     */
    public TestElement createTestElement() {
        ConfigTestElement element = new ConfigTestElement();
        modifyTestElement(element);

        return element;
    }

    /**
     * Sets the consumer sampler properties to the test element.
     *
     * @param element
     */
    public void modifyTestElement(TestElement element) {
        configureTestElement(element);

        element.setProperty(Consumer.URL, setURL.getText());
        element.setProperty(Consumer.DURATION, setDuration.getText());
        element.setProperty(Consumer.RAMP_UP, setRampUp.getText());
        element.setProperty(Consumer.DURABLE, JOrphanUtils.booleanToString(setDurable.isSelected()));
        element.setProperty(Consumer.TOPIC, JOrphanUtils.booleanToString(setTopic.isSelected()));
        element.setProperty(Consumer.NOCONSUMER , setNoConsumer.getText());
        element.setProperty(Consumer.NOSUBJECT, setNoSubject.getText());
        element.setProperty(Consumer.MQSERVER, setMQServer.getSelectedItem().toString());
        element.setProperty(Consumer.TRANSACTED, JOrphanUtils.booleanToString(setTransacted.isSelected()));
        element.setProperty(Consumer.BATCHSIZE, setBatchSize.getText());
    }

    /**
     * Creates the URL panel.
     *
     * @return urlPanel
     */
    private JPanel createURLPanel() {
        JLabel label = new JLabel(JMeterUtils.getResString("form_url"));
        setURL = new JTextField(10);
        setURL.setName(URL);
        label.setLabelFor(setURL);

        JPanel urlPanel = new JPanel(new BorderLayout(5, 0));
        urlPanel.add(label, BorderLayout.WEST);
        urlPanel.add(setURL, BorderLayout.CENTER);

        return urlPanel;
    }

    /**
     * Creates the duration panel.
     *
     * @return durationPanel
     */
    private JPanel createDurationPanel() {
        JLabel label = new JLabel(JMeterUtils.getResString("form_duration"));
        setDuration = new JTextField(10);
        setDuration.setName(DURATION);
        label.setLabelFor(setDuration);

        JPanel durationPanel = new JPanel(new BorderLayout(5, 0));
        durationPanel.add(label, BorderLayout.WEST);
        durationPanel.add(setDuration, BorderLayout.CENTER);

        return durationPanel;
    }

    /**
     * Creates teh ramp up panel.
     *
     * @return rampUpPanel
     */
    private JPanel createRampUpPanel() {
        JLabel label = new JLabel(JMeterUtils.getResString("form_ramp_up"));
        setRampUp = new JTextField(10);
        setRampUp.setName(RAMP_UP);
        label.setLabelFor(setRampUp);

        JPanel rampUpPanel = new JPanel(new BorderLayout(5, 0));
        rampUpPanel.add(label, BorderLayout.WEST);
        rampUpPanel.add(setRampUp, BorderLayout.CENTER);

        return rampUpPanel;
    }

    /**
     * Creates the durable panel.
     *
     * @return durablePanel
     */
    private JPanel createDurablePanel() {
        JLabel labelDeliveryMode = new JLabel(JMeterUtils.getResString("form_delivery_mode"));

        JLabel labelDurable = new JLabel(JMeterUtils.getResString("form_durable"));
        setDurable = new JRadioButton();
        setDurable.setName(DURABLE);
        labelDurable.setLabelFor(setDurable);
        setDurable.setActionCommand(DURABLE);
        setDurable.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent evt) {
                jRadioButtonActionPerformedDelivery(evt);
            }
        });
        setDurable.setSelected(false);

        JLabel labelNonDurable = new JLabel(JMeterUtils.getResString("form_non_durable"));
        setNonDurable = new JRadioButton();
        setNonDurable.setName(NONDURABLE);
        labelNonDurable.setLabelFor(setNonDurable);
        setNonDurable.setActionCommand(NONDURABLE);
        setNonDurable.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent evt) {
                jRadioButtonActionPerformedDelivery(evt);
            }
        });
        setNonDurable.setSelected(true);

        FlowLayout flow = new FlowLayout(FlowLayout.LEFT);
        flow.setHgap(0);
        flow.setVgap(0);

        JPanel durablePanel = new JPanel(flow);
        durablePanel.add(labelDeliveryMode);
        durablePanel.add(new JLabel("  "));
        durablePanel.add(setDurable);
        durablePanel.add(labelDurable);
        durablePanel.add(new JLabel("   "));
        durablePanel.add(setNonDurable);
        durablePanel.add(labelNonDurable);

        return durablePanel;
    }

    /**
     * Creates the topic panel.
     *
     * @return topicPanel
     */
    private JPanel createTopicPanel() {
        JLabel labelMessagingDomain = new JLabel(JMeterUtils.getResString("messaging_domain"));

        JLabel labelTopic = new JLabel(JMeterUtils.getResString("form_topic"));
        setTopic = new JRadioButton();
        setTopic.setName(TOPIC);
        labelTopic.setLabelFor(setTopic);
        setTopic.setActionCommand(TOPIC);
        setTopic.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent evt) {
                jRadioButtonActionPerformed(evt);
            }
        });
        setTopic.setSelected(true);

        JLabel labelQueue = new JLabel(JMeterUtils.getResString("form_queue"));
        setQueue = new JRadioButton();
        setQueue.setName(QUEUE);
        labelQueue.setLabelFor(setQueue);
        setQueue.setActionCommand(QUEUE);
        setQueue.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent evt) {
                jRadioButtonActionPerformed(evt);
            }
        });
        setQueue.setSelected(false);

        FlowLayout flow = new FlowLayout(FlowLayout.LEFT);
        flow.setHgap(0);
        flow.setVgap(0);

        JPanel topicPanel = new JPanel(flow);
        topicPanel.add(labelMessagingDomain);
        topicPanel.add(new JLabel("  "));
        topicPanel.add(setTopic);
        topicPanel.add(labelTopic);
        topicPanel.add(new JLabel("   "));
        topicPanel.add(setQueue);
        topicPanel.add(labelQueue);


        return topicPanel;
    }

    /**
     * Creates the no consumer panel.
     *
     * @return noConsumerPanel
     */
    private JPanel createNoConsumerPanel() {
        JLabel label = new JLabel(JMeterUtils.getResString("form_no_consumer"));
        setNoConsumer = new JTextField(10);
        setNoConsumer.setName(NOCONSUMER);
        label.setLabelFor(setNoConsumer);

        JPanel noConsumerPanel = new JPanel(new BorderLayout(5, 0));
        noConsumerPanel.add(label, BorderLayout.WEST);
        noConsumerPanel.add(setNoConsumer, BorderLayout.CENTER);

        return noConsumerPanel;
    }

    /**
     * Creates the no subject panel.
     *
     * @return noSubjectPanel
     */
    private JPanel createNoSubjectPanel() {
        JLabel label = new JLabel(JMeterUtils.getResString("form_no_subject"));
        setNoSubject = new JTextField(10);
        setNoSubject.setName(NOSUBJECT);
        label.setLabelFor(setNoSubject);

        JPanel noSubjectPanel = new JPanel(new BorderLayout(5, 0));
        noSubjectPanel.add(label, BorderLayout.WEST);
        noSubjectPanel.add(setNoSubject, BorderLayout.CENTER);

        return noSubjectPanel;
    }

    /**
     * Creates the MQ Server Combo Box.
     *
     * @return mqServerPanel
     */
    private JPanel createMQServerPanel() {
        String[] mqServers = {ACTIVEMQ_SERVER,
                              JBOSSMQ_SERVER,
                              SONICMQ_SERVER,
                              TIBCOMQ_SERVER,
                              OPENJMS_SERVER,
                              JORAM_SERVER,
                              MANTARAY_SERVER};

        JLabel label = new JLabel(JMeterUtils.getResString("form_mq_servers"));
        setMQServer = new JComboBox(mqServers);
        setMQServer.setName(MQSERVER);
        label.setLabelFor(setMQServer);

        FlowLayout flow = new FlowLayout(FlowLayout.LEFT);
        flow.setHgap(0);
        flow.setVgap(0);

        JPanel mqServerPanel = new JPanel(flow);
        mqServerPanel.add(label);
        mqServerPanel.add(new JLabel("   "));
        mqServerPanel.add(setMQServer);
        mqServerPanel.add(new JLabel("   "));

        return mqServerPanel;
    }
    /**
     * Creates the Transaction Panel.
     *
     * @return transactionPanel
     */
    private JPanel createTransactedPanel() {
        JLabel labelTransactionType = new JLabel(JMeterUtils.getResString("form_msg_transaction_type"));

        //create Transacted Type
        JLabel labelTransacted = new JLabel(JMeterUtils.getResString("form_transacted"));
        setTransacted = new JRadioButton();
        setTransacted.setName(TRANSACTED);
        labelTransacted.setLabelFor(setTransacted);
        setTransacted.setActionCommand(TRANSACTED);
        setTransacted.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent evt) {
                jRadioActionPerformedTransaction(evt);
            }
        });
        setTransacted.setSelected(false);

        //create Non Transacted Type
        JLabel labelNonTransacted = new JLabel(JMeterUtils.getResString("form_non_transacted"));
        setNonTransacted = new JRadioButton();
        setNonTransacted.setName(NONTRANSACTED);
        labelNonTransacted.setLabelFor(setNonTransacted);
        setNonTransacted.setActionCommand(NONTRANSACTED);
        setNonTransacted.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent evt) {
               jRadioActionPerformedTransaction(evt);
            }
        });

        setNonTransacted.setSelected(true);

        //create Batch Size.
        JLabel labelBatchSize = new JLabel(JMeterUtils.getResString("form_batch_size"));
        setBatchSize = new JTextField(10);
        setBatchSize.setName(BATCHSIZE);
        labelBatchSize.setLabelFor(setBatchSize);

        FlowLayout flow = new FlowLayout(FlowLayout.LEFT);
        flow.setHgap(0);
        flow.setVgap(0);

        JPanel transactionPanel = new JPanel(flow);
        transactionPanel.add(labelTransactionType);
        transactionPanel.add(new JLabel("  "));
        transactionPanel.add(setNonTransacted);
        transactionPanel.add(labelNonTransacted);
        transactionPanel.add(new JLabel("  "));
        transactionPanel.add(setTransacted);
        transactionPanel.add(labelTransacted);
        transactionPanel.add(new JLabel("  "));
        transactionPanel.add(setBatchSize);
        transactionPanel.add(new JLabel("   "));
        transactionPanel.add(labelBatchSize);

        return transactionPanel;
    }

    /**
     * Initializes the gui components.
     */
    private void init() {
        setLayout(new BorderLayout(0, 5));

        if (displayName) {
            setBorder(makeBorder());
            add(makeTitlePanel(), BorderLayout.NORTH);
        }

        VerticalPanel mainPanel = new VerticalPanel();

        mainPanel.add(createURLPanel());
        mainPanel.add(createDurationPanel());
        mainPanel.add(createRampUpPanel());
        mainPanel.add(createNoConsumerPanel());
        mainPanel.add(createNoSubjectPanel());
        mainPanel.add(createDurablePanel());
        mainPanel.add(createTopicPanel());
        mainPanel.add(createTransactedPanel());
        mainPanel.add(createMQServerPanel());

        add(mainPanel, BorderLayout.CENTER);
    }

    /**
     * Listener action for selecting Messaging Domain.
     *
     * @param evt - event triggered.
     */
    private void jRadioButtonActionPerformed(ActionEvent evt) {
        String evtActionCommand = evt.getActionCommand();

        if (evtActionCommand.equals(TOPIC)) {
            setTopic.setSelected(true);
            setQueue.setSelected(false);
            setDurable.setEnabled(true);
            setNonDurable.setEnabled(true);
            setNonDurable.setSelected(true);
        } else if (evtActionCommand.equals(QUEUE)) {
            setTopic.setSelected(false);
            setQueue.setSelected(true);
            setDurable.setSelected(false);
            setDurable.setEnabled(false);
            setNonDurable.setSelected(false);
            setNonDurable.setEnabled(false);
        }
    }

    /**
     * Listener action for selecting Delivery Mode.
     *
     * @param evt - event triggered.
     */
    private void jRadioButtonActionPerformedDelivery(ActionEvent evt) {
        String evtActionCommand = evt.getActionCommand();

        if (evtActionCommand.equals(DURABLE)) {
            setDurable.setSelected(true);
            setNonDurable.setSelected(false);
        } else if (evtActionCommand.equals(NONDURABLE)) {
            setDurable.setSelected(false);
            setNonDurable.setSelected(true);
        }
    }
    /**
     * Listener action for selecting Transaction Type.
     *
     * @param evt - event triggered.
     */
    private void jRadioActionPerformedTransaction(ActionEvent evt) {
        String evtActionCommand = evt.getActionCommand();

        if (evtActionCommand.equals(TRANSACTED)) {
            setTransacted.setSelected(true);
            setNonTransacted.setSelected(false);
            setBatchSize.setEnabled(true);

        } else if (evtActionCommand.equals(NONTRANSACTED)) {
            setTransacted.setSelected(false);
            setNonTransacted.setSelected(true);
            setBatchSize.setEnabled(false);
            setBatchSize.setText("");
        }
    }
}
