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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import com.alibaba.fastjson.JSON;

import com.aliyun.fastmodel.core.parser.DomainLanguage;
import com.aliyun.fastmodel.core.parser.FastModelParser;
import com.aliyun.fastmodel.core.parser.FastModelParserFactory;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.statement.CompositeStatement;
import com.aliyun.fastmodel.driver.server.websocket.model.Message;
import com.aliyun.fastmodel.driver.server.websocket.model.MessageAction;
import com.aliyun.fastmodel.transform.api.Transformer;
import com.aliyun.fastmodel.transform.api.TransformerFactory;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import lombok.extern.slf4j.Slf4j;
import net.sourceforge.plantuml.FileFormat;
import net.sourceforge.plantuml.FileFormatOption;
import net.sourceforge.plantuml.SourceStringReader;
import net.sourceforge.plantuml.code.Base64Coder;
import org.apache.commons.lang3.StringUtils;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/1/31
 */
@Slf4j
public class FastModelWebSocketServer extends WebSocketServer {

    private Transformer<BaseStatement> transformer = TransformerFactory.getInstance().get(
        DialectMeta.getByName(DialectName.PLANTUML));

    private FastModelParser fastModelParser = FastModelParserFactory.getInstance().get();

    public FastModelWebSocketServer(int port) {
        super(new InetSocketAddress(port));
    }

    @Override
    public void onOpen(WebSocket webSocket, ClientHandshake clientHandshake) {
        webSocket.send("WelCome to the Server!");
        broadcast("new Connection:" + clientHandshake.getResourceDescriptor());
    }

    @Override
    public void onClose(WebSocket webSocket, int i, String s, boolean b) {
        broadcast(webSocket + "has left the room!");
        log.info(webSocket + " has left the room");
    }

    @Override
    public void onMessage(WebSocket webSocket, String message) {
        Message message1 = JSON.parseObject(message, Message.class);
        if (message1 == null) {
            return;
        }
        switch (message1.getAction()) {
            case EXECUTE:
                execute(message1);
                break;
            case RENDER:
                render(message1);
                break;
            default:
                log.error("unSupported the message action:" + message1.getAction());
                break;
        }
    }

    private void render(Message message) {
        List<BaseStatement> statements = fastModelParser.multiParse(new DomainLanguage(message.getModel()));
        CompositeStatement compositeStatement = new CompositeStatement(statements);
        DialectNode transform = transformer.transform(compositeStatement);
        SourceStringReader reader = new SourceStringReader(transform.getNode());
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            reader.outputImage(baos, 0, new FileFormatOption(FileFormat.PNG));
            baos.flush();
            final String encodedBytes = "data:image/png;base64,"
                + Base64Coder.encodeLines(baos.toByteArray()).replaceAll("\\s", "");
            baos.close();
            Message returnMessage = new Message(encodedBytes, MessageAction.IMAGE);
            broadcast(JSON.toJSONString(returnMessage));
        } catch (IOException e) {
            log.error("output plantuml error", e);
        }
    }

    private void execute(Message message) {
        if (StringUtils.isBlank(message.getModel())) {
            Message message1 = new Message("model is null", MessageAction.PRINT);
            broadcast(JSON.toJSONString(message1));
            return;
        }
        List<BaseStatement> statements = fastModelParser.multiParse(new DomainLanguage(message.getModel()));
        CompositeStatement compositeStatement = new CompositeStatement(statements);
        DialectNode transform = transformer.transform(compositeStatement);
        Message returnMessage = new Message(transform.getNode(), MessageAction.RENDER);
        broadcast(JSON.toJSONString(returnMessage));
    }

    @Override
    public void onError(WebSocket webSocket, Exception e) {
        log.error("onError", e);
    }

    @Override
    public void onStart() {
        log.info("Server started");
        setConnectionLostTimeout(0);
        setConnectionLostTimeout(100);
    }

    public static void main(String[] args) {
        FastModelWebSocketServer fastModelWebSocketServer = new FastModelWebSocketServer(8887);
        fastModelWebSocketServer.start();
    }
}
