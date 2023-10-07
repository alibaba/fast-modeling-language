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

package com.aliyun.fastmodel.driver.server.websocket;

import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;

import javax.imageio.ImageIO;
import javax.swing.*;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;

import com.alibaba.fastjson.JSON;

import com.aliyun.fastmodel.driver.server.websocket.model.Message;
import com.aliyun.fastmodel.driver.server.websocket.model.MessageAction;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import net.sourceforge.plantuml.SourceStringReader;
import org.apache.commons.lang3.StringUtils;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/2/6
 */
@Slf4j
public class FastModelWebSocketClient extends JFrame implements ActionListener {

    private WebSocketClient cc;

    private JButton connect;

    private JButton close;

    private JTextField uriField;

    private JTextArea ta;

    private JTextArea commandTx;

    private JPanel northPanel;

    private JPanel centerPanel;

    private JPanel southPanel;

    private ImagePanel imagePanel;

    public FastModelWebSocketClient(String defaultLocation) throws HeadlessException {
        super("WebSocket FastModel Client");

        Container c = getContentPane();
        c.setLayout(new BorderLayout());
        initNorthPanel(c, defaultLocation);
        initEastPanel(c);
        initCenterPanel(c);
        initSouthPanel(c);

        addWindowListener(new WindowAdapter() {
            @Override
            public void windowClosing(WindowEvent e) {
                if (cc != null) {
                    cc.close();
                }
                dispose();
            }
        });
        java.awt.Dimension d = new java.awt.Dimension(600, 800);
        setPreferredSize(d);
        setSize(d);

        setLocationRelativeTo(null);
        setVisible(true);
        pack();
    }

    private void initSouthPanel(Container c) {
        southPanel = new JPanel();
        southPanel.setLayout(new FlowLayout());

        JScrollPane jScrollPane = new JScrollPane();
        commandTx = new JTextArea();
        jScrollPane.setViewportView(commandTx);
        jScrollPane.setPreferredSize(new Dimension(400, 300));
        southPanel.add(jScrollPane);
        commandTx.getDocument().addDocumentListener(new DocumentListener() {
            @Override
            public void insertUpdate(DocumentEvent e) {
                renderPanel(e);
            }

            @Override
            public void removeUpdate(DocumentEvent e) {
                renderPanel(e);
            }

            @Override
            public void changedUpdate(DocumentEvent e) {

            }
        });
        c.add(southPanel, BorderLayout.SOUTH);
    }

    private void renderPanel(DocumentEvent e) {
        String text = null;
        try {
            text = e.getDocument().getText(0, e.getDocument().getLength());
        } catch (Exception badLocationException) {
            log.error("getText error", badLocationException);
        }
        if (StringUtils.isNotBlank(text)) {
            Message message = new Message(text, MessageAction.EXECUTE);
            cc.send(JSON.toJSONString(message));
        }
    }

    private void initCenterPanel(Container c) {
        centerPanel = new JPanel();
        JScrollPane jScrollPane = new JScrollPane();
        try {
            BufferedImage imageInputStream = ImageIO.read(
                getClass().getResourceAsStream("/nopic.jpg"));
            imagePanel = new ImagePanel(imageInputStream);
            jScrollPane.setViewportView(imagePanel);
        } catch (IOException e) {
            log.error("can't load the image", e);
        }
        jScrollPane.setPreferredSize(new Dimension(400, 300));
        centerPanel.add(jScrollPane);
        c.add(centerPanel, BorderLayout.CENTER);

    }

    private void initEastPanel(Container c) {
        JScrollPane scroll = new JScrollPane();
        ta = new JTextArea();
        scroll.setViewportView(ta);
        scroll.setPreferredSize(new Dimension(200, 200));
        c.add(scroll, BorderLayout.WEST);
    }

    private void initNorthPanel(Container c, String defaultLocation) {
        northPanel = new JPanel();
        LayoutManager flowLayout = new FlowLayout();
        northPanel.setLayout(flowLayout);
        uriField = new JTextField();
        uriField.setText(defaultLocation);
        northPanel.add(uriField);

        connect = new JButton("Connect");
        connect.addActionListener(this);
        northPanel.add(connect);

        close = new JButton("Close");
        close.addActionListener(this);
        northPanel.add(close);
        c.add(northPanel, BorderLayout.NORTH);
    }

    public static void main(String[] args) {
        new FastModelWebSocketClient("ws://localhost:8887");
    }

    @SneakyThrows
    @Override
    public void actionPerformed(ActionEvent e) {
        if (e.getSource() == connect) {
            cc = new WebSocketClient(new URI(uriField.getText())) {
                @Override
                public void onOpen(ServerHandshake handshakedata) {
                    ta.append("You are connected to Server:" + getURI() + "\n");
                    ta.setCaretPosition(ta.getDocument().getLength());
                }

                @Override
                public void onMessage(String message) {
                    ta.append("got:" + message + "\n");
                    ta.setCaretPosition(ta.getDocument().getLength());
                    try {
                        Message object = JSON.parseObject(message, Message.class);
                        if (object.getAction() == MessageAction.RENDER) {
                            render(object);
                        }
                    } catch (Exception exception) {

                    }
                }

                @Override
                public void onClose(int code, String reason, boolean remote) {
                    ta.append(
                        "You have been disconnected from: " + getURI() + "; Code: " + code + " " + reason
                            + "\n");
                    ta.setCaretPosition(ta.getDocument().getLength());
                    connect.setEnabled(true);
                    uriField.setEditable(true);
                    close.setEnabled(false);
                }

                @Override
                public void onError(Exception ex) {
                    ta.append("Exception occurred ...\n" + ex + "\n");
                    ta.setCaretPosition(ta.getDocument().getLength());
                    ex.printStackTrace();
                    connect.setEnabled(true);
                    uriField.setEditable(true);
                    close.setEnabled(false);
                }
            };

            close.setEnabled(true);
            connect.setEnabled(false);
            uriField.setEditable(false);
            cc.connect();
        } else if (e.getSource() == close) {
            cc.close();
        }
    }

    private void render(Message message) {
        SourceStringReader sourceStringReader = new SourceStringReader(message.getModel());
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try {
            sourceStringReader.outputImage(byteArrayOutputStream);
            byteArrayOutputStream.close();
            BufferedImage bufferedImage = ImageIO.read(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()));
            imagePanel.setImage(bufferedImage);
            imagePanel.repaint();
        } catch (IOException e) {
            log.error("render image error", e);
        }

    }

}
