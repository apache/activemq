/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.activemq.sampler.config.gui;

import org.apache.jmeter.config.ConfigTestElement;
import org.apache.jmeter.config.gui.AbstractConfigGui;
import org.apache.jmeter.gui.util.VerticalPanel;
import org.apache.jmeter.testelement.TestElement;
import org.apache.jmeter.util.JMeterUtils;
import org.apache.jorphan.util.JOrphanUtils;
import org.activemq.sampler.Producer;
import org.activemq.sampler.ProducerSysTest;

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
 * Producer configuration gui bean.
 */
public class ProducerSysTestConfigGui extends AbstractConfigGui {

    private static final String URL = "url";
    private static final String DURABLE = "durable";
    private static final String NONDURABLE = "nondurable";
    private static final String TOPIC = "topic";
    private static final String QUEUE = "queue";
    private static final String NOPRODUCER = "noproducer";
    private static final String NOSUBJECT = "nosubject";
    private static final String PRODUCER_SYS_TEST_CONFIG_TITLE = "producer_sys_test_config_title";
    private static final String NOMESSAGES = "nomessages";


    private JTextField setURL;
    private JTextField setNoProducer;
    private JTextField setNoSubject;
    private JRadioButton setDurable;
    private JRadioButton setNonDurable;
    private JRadioButton setTopic;
    private JRadioButton setQueue;
    private JTextField setNoMessages;

    private boolean displayName = true;

    /**
     * Default constructor.
     */
    public ProducerSysTestConfigGui() {
        this(true);
    }

    /**
     * Constructor.
     *
     * @param displayName - whether to display the name of the producer.
     */
    public ProducerSysTestConfigGui(boolean displayName) {
        this.displayName = displayName;
        init();
    }

    /**
     * Returns the producer configuration title.
     *
     * @return producer configuration title
     */
    public String getLabelResource() {
        return PRODUCER_SYS_TEST_CONFIG_TITLE;
    }

    /**
     * Configures the ProducerConfigGui bean.
     *
     * @param element - producer sampler properties.
     */
    public void configure(TestElement element) {
        super.configure(element);

        setURL.setText(element.getPropertyAsString(ProducerSysTest.URL));
        //setDuration.setText(element.getPropertyAsString(ProducerSysTest.DURATION));
        //setRampUp.setText(element.getPropertyAsString(ProducerSysTest.RAMP_UP));

        if (element.getProperty(ProducerSysTest.DURABLE) == null) {
            setDurable.setSelected(true);
            setNonDurable.setSelected(false);
        } else {
            if (element.getPropertyAsBoolean(ProducerSysTest.DURABLE)) {
                setDurable.setSelected(true);
                setNonDurable.setSelected(false);
            } else {
                setDurable.setSelected(false);
                setNonDurable.setSelected(true);
            }
        }

        if (element.getProperty(ProducerSysTest.TOPIC) == null) {
            setTopic.setSelected(true);
            setQueue.setSelected(false);
        } else {
            if (element.getPropertyAsBoolean(ProducerSysTest.TOPIC)) {
                setTopic.setSelected(true);
                setQueue.setSelected(false);
            } else {
                setTopic.setSelected(false);
                setQueue.setSelected(true);
            }
        }

        //setMsgSize.setText(element.getPropertyAsString(ProducerSysTest.MSGSIZE));
        setNoProducer.setText(element.getPropertyAsString(ProducerSysTest.NOPRODUCER));
        setNoSubject.setText(element.getPropertyAsString(ProducerSysTest.NOSUBJECT));
        //setMsgSize.setText(element.getPropertyAsString(ProducerSysTest.MSGSIZE));
        setNoMessages.setText(element.getPropertyAsString(ProducerSysTest.NOMESSAGES));

        /*
        if (element.getProperty(ProducerSysTest.DEFMSGINTERVAL) == null) {
            setDefMsgInterval.setSelected(true);
            setCusMsgInterval.setSelected(false);
            setMsgInterval.setEnabled(false);
        } else {
            if (element.getPropertyAsBoolean(ProducerSysTest.DEFMSGINTERVAL)) {
                setDefMsgInterval.setSelected(true);
                setCusMsgInterval.setSelected(false);
                setMsgInterval.setEnabled(false);
            } else {
                setDefMsgInterval.setSelected(false);
                setCusMsgInterval.setSelected(true);
                setMsgInterval.setEnabled(true);
                setMsgInterval.setText(element.getPropertyAsString(ProducerSysTest.MSGINTERVAL));
            }
        }
        */
        /*
        setMQServer.setSelectedItem(element.getPropertyAsString(ProducerSysTest.MQSERVER));


        if (element.getProperty(ProducerSysTest.TRANSACTED) == null) {
            setTransacted.setSelected(false);
            setNonTransacted.setSelected(true);
            setBatchSize.setEnabled(false);
        } else {
            if (element.getPropertyAsBoolean(ProducerSysTest.TRANSACTED)) {
                setTransacted.setSelected(true);
                setNonTransacted.setSelected(false);
                setBatchSize.setEnabled(true);
                setBatchSize.setText(element.getPropertyAsString(ProducerSysTest.BATCHSIZE));
            } else {
                setTransacted.setSelected(false);
                setNonTransacted.setSelected(true);
                setBatchSize.setEnabled(false);
                setBatchSize.setText("");
            }
        }
        */

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
     * Sets the producer sampler properties to the test element.
     *
     * @param element
     */
    public void modifyTestElement(TestElement element) {
        configureTestElement(element);

        element.setProperty(ProducerSysTest.URL, setURL.getText());
        //element.setProperty(ProducerSysTest.DURATION, setDuration.getText());
        //element.setProperty(ProducerSysTest.RAMP_UP, setRampUp.getText());
        element.setProperty(ProducerSysTest.DURABLE, JOrphanUtils.booleanToString(setDurable.isSelected()));
        element.setProperty(ProducerSysTest.TOPIC, JOrphanUtils.booleanToString(setTopic.isSelected()));
        //element.setProperty(ProducerSysTest.MSGSIZE, setMsgSize.getText());
        element.setProperty(ProducerSysTest.NOPRODUCER, setNoProducer.getText());
        element.setProperty(ProducerSysTest.NOSUBJECT, setNoSubject.getText());
        element.setProperty(ProducerSysTest.NOMESSAGES, setNoMessages.getText());
        //element.setProperty(ProducerSysTest.DEFMSGINTERVAL, JOrphanUtils.booleanToString(setDefMsgInterval.isSelected()));
        //element.setProperty(ProducerSysTest.MSGINTERVAL, setMsgInterval.getText());
        //element.setProperty(ProducerSysTest.MQSERVER, setMQServer.getSelectedItem().toString());
        //element.setProperty(ProducerSysTest.TRANSACTED, JOrphanUtils.booleanToString(setTransacted.isSelected()));
        //element.setProperty(ProducerSysTest.BATCHSIZE, setBatchSize.getText());
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
     * Creates the durable panel.
     *
     * @return durablePanel
     */
    private JPanel createDurablePanel() {
        JLabel labelDeliveryMode = new JLabel(JMeterUtils.getResString("form_delivery_mode"));

        JLabel labelDurable = new JLabel(JMeterUtils.getResString("form_persistent"));
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

        JLabel labelNonDurable = new JLabel(JMeterUtils.getResString("form_non_persistent"));
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
     * Creates the no prod panel.
     *
     * @return noProdPanel
     */
    private JPanel createNoProducerPanel() {
        JLabel label = new JLabel(JMeterUtils.getResString("form_no_producer"));
        setNoProducer = new JTextField(10);
        setNoProducer.setName(NOPRODUCER);
        label.setLabelFor(setNoProducer);

        JPanel noProdPanel = new JPanel(new BorderLayout(5, 0));
        noProdPanel.add(label, BorderLayout.WEST);
        noProdPanel.add(setNoProducer, BorderLayout.CENTER);

        return noProdPanel;
    }

    /**
     * Creates the number subject panel.
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
     * Creates the number of messages panel.
     *
     * @return noMessagesPanel
     */
    private JPanel createNoMessagesPanel() {
        JLabel label = new JLabel(JMeterUtils.getResString("form_no_messages"));
        setNoMessages = new JTextField(10);
        setNoMessages.setName(NOMESSAGES);
        label.setLabelFor(setNoMessages);

        JPanel noMessagesPanel = new JPanel(new BorderLayout(5, 0));
        noMessagesPanel.add(label, BorderLayout.WEST);
        noMessagesPanel.add(setNoMessages, BorderLayout.CENTER);

        return noMessagesPanel;
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
        //mainPanel.add(createDurationPanel());
        //mainPanel.add(createRampUpPanel());
        mainPanel.add(createNoProducerPanel());
        mainPanel.add(createNoSubjectPanel());
        mainPanel.add(createNoMessagesPanel());
        //mainPanel.add(createMsgSizePanel());
        mainPanel.add(createDurablePanel());
        mainPanel.add(createTopicPanel());
        //mainPanel.add(createTransactedPanel());
        //mainPanel.add(createDefMsgIntervalPanel());
        //mainPanel.add(createMQServerPanel());

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
        } else if (evtActionCommand.equals(QUEUE)) {
            setTopic.setSelected(false);
            setQueue.setSelected(true);
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

}
