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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.util.Arrays;
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
 * HTTPS example server. Set FML_HTTPS_KEYSTORE_PATH to a local PKCS12 keystore and
 * FML_HTTPS_KEYSTORE_PASSWORD to its password before starting the server.
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
        String keyStorePath = System.getenv("FML_HTTPS_KEYSTORE_PATH");
        String keyStorePassword = System.getenv("FML_HTTPS_KEYSTORE_PASSWORD");
        if (keyStorePath == null || keyStorePath.trim().isEmpty()
            || keyStorePassword == null || keyStorePassword.isEmpty()) {
            throw new IllegalStateException("Set FML_HTTPS_KEYSTORE_PATH and FML_HTTPS_KEYSTORE_PASSWORD");
        }

        SSLContext sslContext = SSLContext.getInstance("TLS");
        char[] password = keyStorePassword.toCharArray();
        try {
            KeyStore ks = KeyStore.getInstance("PKCS12");
            try (InputStream input = Files.newInputStream(Paths.get(keyStorePath))) {
                ks.load(input, password);
            }

            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(ks, password);
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(ks);
            sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
        } finally {
            Arrays.fill(password, '\0');
        }

        // Validate TLS configuration before binding a listening socket.
        InetSocketAddress address = new InetSocketAddress(InetAddress.getLocalHost(), port);
        httpsServer = HttpsServer.create(address, 0);

        httpsServer.setHttpsConfigurator(
            new HttpsConfigurator(sslContext) {
                @Override
                public void configure(HttpsParameters httpsParameters) {
                    SSLEngine engine = sslContext.createSSLEngine();
                    httpsParameters.setNeedClientAuth(false);
                    httpsParameters.setCipherSuites(engine.getEnabledCipherSuites());
                    httpsParameters.setProtocols(engine.getEnabledProtocols());
                    httpsParameters.setSSLParameters(sslContext.getDefaultSSLParameters());

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
