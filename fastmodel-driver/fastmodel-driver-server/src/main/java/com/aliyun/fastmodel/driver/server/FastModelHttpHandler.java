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

package com.aliyun.fastmodel.driver.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;

import com.alibaba.fastjson.JSON;

import com.aliyun.fastmodel.core.exception.ParseException;
import com.aliyun.fastmodel.core.parser.DomainLanguage;
import com.aliyun.fastmodel.core.parser.FastModelParser;
import com.aliyun.fastmodel.core.parser.FastModelParserFactory;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.driver.client.request.FastModelRequestBody;
import com.aliyun.fastmodel.driver.model.DriverResult;
import com.aliyun.fastmodel.driver.server.command.CommandFactory;
import com.aliyun.fastmodel.driver.server.command.FmlCommand;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;

/**
 * JdbcHttpHandler: Handler
 *
 * @author panguanjing
 * @date 2020/12/9
 */
@Slf4j
public class FastModelHttpHandler implements HttpHandler {

    private static final FastModelParser PARSER = FastModelParserFactory.getInstance().get();

    @Override
    public void handle(HttpExchange httpExchange) throws IOException {
        //获取到sql
        String language = getLanguage(httpExchange);
        if (language == null) {
            handleResponse(httpExchange,
                new ResultWrapper(DriverResult.error("can't parse the statement from request body")));
            return;
        }
        //使用parser进行解析
        try {
            BaseStatement parse = PARSER.parse(new DomainLanguage(language));
            if (parse == null) {
                handleResponse(httpExchange, new ResultWrapper(DriverResult.error("statement is null")));
                return;
            }
            //解析成功，根据语句类型，转到不同的command进行处理
            FmlCommand fmlCommand = CommandFactory.INSTANCE.getCommand(parse);
            if (fmlCommand == null) {
                handleResponse(httpExchange,
                    new ResultWrapper(DriverResult.error("cannot find command with statement")));
                return;
            }
            DriverResult execute = fmlCommand.execute(parse);
            //直接返回json处理
            handleResponse(httpExchange, new ResultWrapper(execute));
        } catch (ParseException e) {
            //如果解析失败，直接返回处理
            DriverResult driverResult = new DriverResult();
            driverResult.setSuccess(false);
            driverResult.setErrorMessage(e.getMessage());
            handleResponse(httpExchange, new ResultWrapper(driverResult));
        }

    }

    private String getLanguage(HttpExchange query) {
        InputStream requestBody = query.getRequestBody();
        if (requestBody == null) {
            return null;
        }
        StringWriter output = new StringWriter();
        try {
            IOUtils.copy(requestBody, output, StandardCharsets.UTF_8);
            FastModelRequestBody requestBody2 = JSON.parseObject(output.toString(), FastModelRequestBody.class);
            return requestBody2.getFml();
        } catch (IOException e) {
            return null;
        }
    }

    private void handleResponse(HttpExchange httpExchange, ResultWrapper execute) throws IOException {
        OutputStream outputStream = httpExchange.getResponseBody();
        httpExchange.getResponseHeaders().set("Content-Type", "application/json");
        httpExchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        String htmlResponse = JSON.toJSONString(execute);
        if (execute.getData().isSuccess()) {
            httpExchange.sendResponseHeaders(200, htmlResponse.length());
        } else {
            httpExchange.sendResponseHeaders(500, htmlResponse.length());
        }
        log.info("write the result:{}", htmlResponse);
        outputStream.write(htmlResponse.getBytes());
        outputStream.flush();
        outputStream.close();
    }

}
