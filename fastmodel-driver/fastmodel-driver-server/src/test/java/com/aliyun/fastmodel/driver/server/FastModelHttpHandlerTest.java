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
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.BDDMockito.given;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/12/9
 */
@RunWith(MockitoJUnitRunner.class)
public class FastModelHttpHandlerTest {

    @Mock
    HttpExchange httpExchange;

    @Test
    public void testHandler() throws IOException, URISyntaxException {
        FastModelHttpHandler fastModelHttpHandler = new FastModelHttpHandler();
        given(httpExchange.getRequestURI()).willReturn(new URI("http://localhost:3030"));
        given(httpExchange.getResponseBody()).willReturn(Mockito.mock(OutputStream.class));
        Headers headers = Mockito.mock(Headers.class);
        given(httpExchange.getResponseHeaders()).willReturn(headers);
        fastModelHttpHandler.handle(httpExchange);
    }
}