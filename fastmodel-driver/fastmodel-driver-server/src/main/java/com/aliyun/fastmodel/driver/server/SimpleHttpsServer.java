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

import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManagerFactory;

import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.HttpsServer;
import lombok.extern.slf4j.Slf4j;

/**
 * Https Server
 *
 * @author panguanjing
 * @date 2020/12/25
 */
@Slf4j
public class SimpleHttpsServer {
    private ThreadPoolExecutor executor = null;

    private int port = 0;
    private HttpsServer httpsServer;

    public SimpleHttpsServer(int port) {
        this.port = port;
    }

    public void startHttps()
        throws Exception {
        InetSocketAddress address = new InetSocketAddress(InetAddress.getLocalHost(), port);
        //init
        httpsServer = HttpsServer.create(address, 0);

        SSLContext sslContext = SSLContext.getInstance("TLS");

        //init the keystore
        char[] password = "simulator".toCharArray();
        KeyStore ks = KeyStore.getInstance("JKS");

        //FileInputStream fis = new FileInputStream("lig.keystore");
        InputStream fis = this.getClass().getResourceAsStream("/lig.keystore");
        ks.load(fis, password);

        //set up the key manager factory
        KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
        kmf.init(ks, password);

        //set up the trust manager factory
        TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
        tmf.init(ks);

        //set up the Https context and parameters
        sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);

        httpsServer.setHttpsConfigurator(
            new HttpsConfigurator(sslContext) {
                @Override
                public void configure(HttpsParameters httpsParameters) {
                    try {
                        SSLContext c = SSLContext.getDefault();
                        SSLEngine engine = c.createSSLEngine();
                        httpsParameters.setNeedClientAuth(false);
                        httpsParameters.setCipherSuites(engine.getEnabledCipherSuites());
                        httpsParameters.setProtocols(engine.getEnabledProtocols());

                        httpsParameters.setSSLParameters(c.getDefaultSSLParameters());
                    } catch (NoSuchAlgorithmException e) {
                        log.error("occur exception", e);
                    }

                }
            }
        );
        httpsServer.createContext("/test", new MyHandler());
        httpsServer.createContext("/jdbc", new FastModelHttpHandler());
        executor = new ThreadPoolExecutor(
            10, 10, 30, TimeUnit.SECONDS,
            new SynchronousQueue<>(true),
            r -> new Thread(r, "Custom")
        );
        httpsServer.setExecutor(executor);
        httpsServer.start();
        log.info("start https server with port:{}", port);
    }

    public static void main(String[] args) {
        try {
            new SimpleHttpsServer(18080).startHttps();
        } catch (Exception e) {
            log.error("start https error", e);
        }
    }

    public void close() {
        if (httpsServer != null) {
            httpsServer.stop(1);
        }
        if (executor != null) {
            executor.shutdownNow();
        }
    }
}
