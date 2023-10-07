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
import java.net.InetSocketAddress;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.sun.net.httpserver.HttpServer;
import lombok.extern.slf4j.Slf4j;

/**
 * 最简单的http服务，用于结合
 *
 * @author panguanjing
 * @date 2020/12/9
 */
@Slf4j
public class SimpleHttp {

    private final String host;
    private final int port;
    private HttpServer httpServer = null;
    private ThreadPoolExecutor executor = null;

    public SimpleHttp(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void start() throws IOException {
        httpServer = HttpServer.create(
            new InetSocketAddress(host, port), 0
        );
        httpServer.createContext("/jdbc", new FastModelHttpHandler());
        executor = new ThreadPoolExecutor(
            10, 10, 30, TimeUnit.SECONDS,
            new SynchronousQueue<>(true),
            r -> new Thread(r, "Custom")
        );
        httpServer.setExecutor(executor);
        httpServer.start();
        log.info("start the httpserver at:" + port);
    }

    public void stop() {
        if (httpServer != null) {
            httpServer.stop(1);
        }
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    public static void main(String[] args) throws IOException {
        SimpleHttp simpleHttp = new SimpleHttp("localhost", 8080);
        simpleHttp.start();
    }
}
