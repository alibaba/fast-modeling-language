/*
 * Copyright 2021-2022 Alibaba Group Holding Ltd.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.aliyun.fastmodel.transform.example.ui;

import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.*;

import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.api.dialect.transform.DialectTransform;
import com.aliyun.fastmodel.transform.api.dialect.transform.DialectTransformParam;
import jsyntaxpane.DefaultSyntaxKit;

/**
 * zen converter的处理内容
 *
 * @author panguanjing
 * @date 2021/7/17
 */
public class SqlTransformFrame extends JFrame implements ActionListener {

    JSplitPane splitPane;

    JEditorPane leftTextPanel;

    JEditorPane rightTextPanel;

    JComboBox leftCombox;

    JComboBox rightCombox;

    public static final Object[] OBJECTS = {"Oracle", "Mysql", "MaxCompute", "Hive", "Hologres", "FML"};

    public SqlTransformFrame(String title) throws HeadlessException {
        super(title);
        DefaultSyntaxKit.initKit();
        init();
        leftTextPanel.setContentType("text/sql");
        rightTextPanel.setContentType("text/sql");
    }

    private void init() {
        JPanel left = initLeftPanel();
        JPanel right = initRightPanel();
        splitPane = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, left, right);
        Container container = getContentPane();
        container.add(splitPane, BorderLayout.CENTER);
        setPreferredSize(new Dimension(800, 600));
        setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
    }

    private JPanel initRightPanel() {
        JPanel rightPanel = new JPanel();
        JPanel selectPanel = new JPanel();
        JLabel jLabel = new JLabel("Target:");
        rightCombox = new JComboBox(OBJECTS);
        rightCombox.setSelectedIndex(1);
        selectPanel.setLayout(new FlowLayout());
        selectPanel.add(jLabel);
        selectPanel.add(rightCombox);
        rightPanel.setLayout(new BorderLayout());
        rightPanel.add(selectPanel, BorderLayout.NORTH);

        rightTextPanel = new JEditorPane();
        JScrollPane jScrollPane = new JScrollPane(rightTextPanel);
        rightPanel.add(jScrollPane, BorderLayout.CENTER);
        return rightPanel;
    }

    private JPanel initLeftPanel() {
        JPanel leftPanel = new JPanel();
        JPanel selectPanel = new JPanel();
        JLabel jLabel = new JLabel("Source:");
        leftCombox = new JComboBox(OBJECTS);
        JButton convert = new JButton("Convert");
        selectPanel.setLayout(new FlowLayout());
        selectPanel.add(jLabel);
        selectPanel.add(leftCombox);
        selectPanel.add(convert);

        convert.addActionListener(this);
        leftPanel.setLayout(new BorderLayout());
        leftPanel.add(selectPanel, BorderLayout.NORTH);

        leftTextPanel = new JEditorPane();
        JScrollPane jScrollPane = new JScrollPane(leftTextPanel);
        leftPanel.add(jScrollPane, BorderLayout.CENTER);
        return leftPanel;
    }

    public static void main(String[] args) {
        SqlTransformFrame sqlTransformFrame = new SqlTransformFrame("FastModel DDL Transform");
        sqlTransformFrame.pack();
        sqlTransformFrame.setLocationRelativeTo(null);
        sqlTransformFrame.setVisible(true);
    }

    @Override
    public void actionPerformed(ActionEvent e) {
        String text = leftTextPanel.getText();
        Object selectedItem = leftCombox.getSelectedItem();
        Object rightSelectItem = rightCombox.getSelectedItem();
        DialectTransformParam build = DialectTransformParam.builder()
            .sourceMeta(DialectMeta.getByNameAndVersion(selectedItem.toString(), DialectMeta.DEFAULT_VERSION))
            .sourceNode(new DialectNode(text))
            .targetMeta(DialectMeta.getByNameAndVersion(rightSelectItem.toString(), DialectMeta.DEFAULT_VERSION))
            .build();
        DialectNode transform = DialectTransform.transform(build);
        rightTextPanel.setText(transform.getNode());
    }

}
