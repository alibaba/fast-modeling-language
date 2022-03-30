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

package com.aliyun.aliyun.transform.zen.ui;

import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.List;
import java.util.stream.Collectors;

import javax.swing.*;

import com.aliyun.aliyun.transform.zen.converter.ZenNodeConverter;
import com.aliyun.aliyun.transform.zen.converter.ZenNodeConverterFactory;
import com.aliyun.aliyun.transform.zen.parser.ZenParser;
import com.aliyun.aliyun.transform.zen.parser.ZenParserImpl;
import com.aliyun.aliyun.transform.zen.parser.tree.BaseZenNode;
import com.aliyun.fastmodel.core.formatter.FastModelFormatter;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import org.apache.commons.lang3.StringUtils;

/**
 * zen converter的处理内容
 *
 * @author panguanjing
 * @date 2021/7/17
 */
public class ZenConverterFrame extends JFrame implements ActionListener {

    private JTextArea input;
    private JTextArea jTextArea;
    private JButton jButton;

    public ZenConverterFrame(String title) throws HeadlessException {
        super(title);
        init();
    }

    private void init() {
        input = new JTextArea(10, 24);
        jTextArea = new JTextArea();
        jButton = new JButton("Submit");

        JPanel jPanel = new JPanel();
        jPanel.add(input);
        jPanel.add(jButton);

        jButton.addActionListener(this);
        Container container = getContentPane();
        container.add(jPanel, BorderLayout.NORTH);
        container.add(jTextArea, BorderLayout.CENTER);
        setPreferredSize(new Dimension(400, 600));
        setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
    }

    public static void main(String[] args) {
        ZenConverterFrame zenConverterFrame = new ZenConverterFrame("ZenCode");
        zenConverterFrame.setVisible(true);
        zenConverterFrame.pack();
    }

    @Override
    public void actionPerformed(ActionEvent e) {
        if (e.getSource() == jButton) {
            invoke();
        }
    }

    private void invoke() {
        String input = this.input.getText();
        if (StringUtils.isBlank(input)) {
            return;
        }
        ZenParser zenParser = new ZenParserImpl();
        BaseZenNode baseZenNode = zenParser.parseNode(input.trim());
        ZenNodeConverter zenNodeConverter = ZenNodeConverterFactory.getInstance().create(ColumnDefinition.class);
        List<ColumnDefinition> convert = zenNodeConverter.convert(baseZenNode, null);
        String format = convert.stream().map(x -> {
            return FastModelFormatter.formatNode(x);
        }).collect(Collectors.joining(",\n"));
        jTextArea.setText(format);
    }
}
