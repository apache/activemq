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
import org.activemq.sampler.ConsumerSysTest;


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
public class ConsumerSysTestConfigGui extends AbstractConfigGui {

    //private final static String FILENAME = "filename";
    private final static String URL = "url";
    private final static String DURABLE = "durable";
    private final static String NONDURABLE = "nondurable";
    private final static String TOPIC = "topic";
    private final static String QUEUE = "queue";
    private final static String NOCONSUMER = "noconsumer";
    private final static String NOSUBJECT = "nosubject";
    private final static String CONSUMER_SYS_TEST_CONFIG_TITLE = "consumer_sys_test_config_title";

    private JTextField setURL;
    private JTextField setNoConsumer;
    private JTextField setNoSubject;

    private JRadioButton setDurable;
    private JRadioButton setNonDurable;
    private JRadioButton setTopic;
    private JRadioButton setQueue;

    private boolean displayName = true;

    /**
     * Default constructor.
     */
    public ConsumerSysTestConfigGui() {
        this(true);
    }

    /**
     * Constructor.
     *
     * @param displayName - whether to display the name of the consumer.
     */
    public ConsumerSysTestConfigGui(boolean displayName) {
        this.displayName = displayName;
        init();
    }

    /**
     * Returns the consumer configuration title.
     *
     * @return consumer configuration title
     */
    public String getLabelResource() {
        return CONSUMER_SYS_TEST_CONFIG_TITLE;
    }

    /**
     * Configures the ConsumerConfigGui bean.
     *
     * @param element - consumer sampler properties.
     */
    public void configure(TestElement element) {
        super.configure(element);

        setURL.setText(element.getPropertyAsString(ConsumerSysTest.URL));

        if (element.getProperty(ConsumerSysTest.DURABLE) == null) {
            setDurable.setSelected(false);
            setNonDurable.setSelected(true);
        } else {
            if (element.getPropertyAsBoolean(ConsumerSysTest.DURABLE)) {
                setDurable.setSelected(true);
                setNonDurable.setSelected(false);
            } else {
                setDurable.setSelected(false);
                setNonDurable.setSelected(true);
            }
        }

        if (element.getProperty(ConsumerSysTest.TOPIC) == null) {
            setTopic.setSelected(true);
            setQueue.setSelected(false);
        } else {
            if (element.getPropertyAsBoolean(ConsumerSysTest.TOPIC)) {
                setTopic.setSelected(true);
                setQueue.setSelected(false);
            } else {
                setTopic.setSelected(false);
                setQueue.setSelected(true);
            }
        }

        setNoConsumer.setText(element.getPropertyAsString(ConsumerSysTest.NOCONSUMER));
        setNoSubject.setText(element.getPropertyAsString(ConsumerSysTest.NOSUBJECT));
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

        element.setProperty(ConsumerSysTest.URL, setURL.getText());
        element.setProperty(ConsumerSysTest.DURABLE, JOrphanUtils.booleanToString(setDurable.isSelected()));
        element.setProperty(ConsumerSysTest.TOPIC, JOrphanUtils.booleanToString(setTopic.isSelected()));
        element.setProperty(ConsumerSysTest.NOCONSUMER , setNoConsumer.getText());
        element.setProperty(ConsumerSysTest.NOSUBJECT, setNoSubject.getText());
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
        mainPanel.add(createNoConsumerPanel());
        mainPanel.add(createNoSubjectPanel());
        mainPanel.add(createDurablePanel());
        mainPanel.add(createTopicPanel());

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

}
