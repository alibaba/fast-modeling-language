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

package com.aliyun.fastmodel.driver.client.command.tenant;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import com.aliyun.fastmodel.driver.client.command.SslMode;
import com.aliyun.fastmodel.driver.client.utils.ssl.NonValidatingTrustManager;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
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
public class TenantHttpClientBuilder {
    private final TenantProperties properties;

    public TenantHttpClientBuilder(TenantProperties info) {
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

        if (BooleanUtils.isTrue(properties.getSsl())) {
            HostnameVerifier verifier = "strict".equals(properties.getSslMode()) ? SSLConnectionSocketFactory
                .getDefaultHostnameVerifier() :
                NoopHostnameVerifier.INSTANCE;
            registry.register("https", new SSLConnectionSocketFactory(getSSLContext(), verifier));
        }

        return new PoolingHttpClientConnectionManager(
            registry.build(),
            null,
            null,
            new SystemDefaultDnsResolver(),
            properties.getTimeToLiveMillis(),
            TimeUnit.MILLISECONDS
        );
    }

    private SSLContext getSSLContext()
        throws NoSuchAlgorithmException, KeyManagementException, CertificateException, KeyStoreException, IOException {
        SSLContext ctx = SSLContext.getInstance("TLS");
        TrustManager[] tms = null;
        KeyManager[] kms = null;
        SecureRandom sr = null;
        if (properties.getSslMode().equalsIgnoreCase(SslMode.NONE.name())) {
            tms = new TrustManager[] {new NonValidatingTrustManager()};
            kms = new KeyManager[] {};
            sr = new SecureRandom();
        } else if (properties.getSslMode().equalsIgnoreCase(SslMode.STRICT.name())) {
            if (!properties.getSslRootCertificate().isEmpty()) {
                TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                tmf.init(getKeyStore());
                tms = tmf.getTrustManagers();
                kms = new KeyManager[] {};
                sr = new SecureRandom();
            }
        } else {
            throw new IllegalArgumentException("unknown ssl model '" + properties.getSslMode() + "'");
        }
        ctx.init(kms, tms, sr);
        return ctx;
    }

    private KeyStore getKeyStore() throws NoSuchAlgorithmException, IOException, CertificateException,
        KeyStoreException {
        KeyStore ks;
        try {
            ks = KeyStore.getInstance("jks");
            ks.load(null, null);
        } catch (KeyStoreException e) {
            throw new NoSuchAlgorithmException("jks keystore not available");
        }
        InputStream caInputStream;
        try {
            caInputStream = new FileInputStream(properties.getSslRootCertificate());
        } catch (FileNotFoundException e) {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            caInputStream = classLoader.getResourceAsStream(properties.getSslRootCertificate());
            if (caInputStream == null) {
                throw new IOException(
                    "Could not open SSL/TLS root certificate file: " + properties.getSslRootCertificate(), e);
            }
        }
        try {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            Iterator<? extends Certificate> iterator = cf.generateCertificates(caInputStream).iterator();
            for (int i = 0; iterator.hasNext(); i++) {
                ks.setCertificateEntry("cert" + i, iterator.next());
            }
            return ks;
        } finally {
            caInputStream.close();
        }
    }
}
