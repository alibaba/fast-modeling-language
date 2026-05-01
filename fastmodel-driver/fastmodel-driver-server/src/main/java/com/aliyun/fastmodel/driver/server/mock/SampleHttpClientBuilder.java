/*
 * Copyright (c)  2021. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.driver.server.mock;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;

import com.aliyun.fastmodel.driver.client.utils.ssl.NonValidatingTrustManager;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.impl.conn.SystemDefaultDnsResolver;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/12/28
 */
public class SampleHttpClientBuilder {
    private final MockCommandProperties properties;

    public SampleHttpClientBuilder(MockCommandProperties info) {
        properties = info;
    }

    public CloseableHttpClient buildClient() throws Exception {
        return HttpClientBuilder.create().setConnectionManager(getConnectionManager()).disableContentCompression().
            build();
    }

    private HttpClientConnectionManager getConnectionManager()
        throws KeyManagementException, NoSuchAlgorithmException, CertificateException, KeyStoreException, IOException {
        RegistryBuilder<ConnectionSocketFactory> registry = RegistryBuilder.<ConnectionSocketFactory>create().register(
            "http", PlainConnectionSocketFactory.getSocketFactory()
        );

        return new PoolingHttpClientConnectionManager(
            registry.build(),
            null,
            null,
            new SystemDefaultDnsResolver(),
            1000,
            TimeUnit.MILLISECONDS
        );
    }

    private SSLContext getSSLContext()
        throws NoSuchAlgorithmException, KeyManagementException, CertificateException, KeyStoreException, IOException {
        SSLContext ctx = SSLContext.getInstance("TLS");
        TrustManager[] tms = null;
        KeyManager[] kms = null;
        SecureRandom sr = null;
        tms = new TrustManager[] {new NonValidatingTrustManager()};
        kms = new KeyManager[] {};
        sr = new SecureRandom();
        ctx.init(kms, tms, sr);
        return ctx;
    }

}
