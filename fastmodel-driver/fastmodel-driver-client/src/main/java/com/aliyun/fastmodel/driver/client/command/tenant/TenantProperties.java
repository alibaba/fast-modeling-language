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

import java.util.Properties;

import com.aliyun.fastmodel.driver.client.command.BaseCommandProperties;
import lombok.Getter;
import lombok.Setter;

/**
 * @author panguanjing
 * @date 2021/3/25
 */
@Getter
@Setter
public class TenantProperties extends BaseCommandProperties {
    private String baseKey;
    private String token;
    private Boolean ssl;
    private String sslMode;
    private String tenantId;
    private String path;
    private Integer keepAliveTimeout;
    private Integer connectionTimeout;
    private Integer timeToLiveMillis;
    private String sslRootCertificate;
    private String buCode;

    public static final String PATH = "path";
    public static final String KEEP_ALIVE_TIMEOUT = "keepAliveTimeout";
    public static final String CONNECTION_TIMEOUT = "connectionTimeout";
    public static final String PROJECT = "project";
    public static final String SSL = "ssl";
    public static final String TOKEN = "token";
    public static final String BASE_KEY = "baseKey";
    public static final String TENANT_ID = "tenantId";
    public static final String SSL_MODE = "sslMode";
    public static final String SSL_ROOT_CERTIFICATE = "sslRootCertificate";
    public static final String TIME_TO_LIVE_MILLIS = "timeToLiveMillis";
    public static final String BU_CODE = "buCode";

    public TenantProperties(Properties properties) {
        super(properties);
        ssl = getSetting(properties, SSL, true, Boolean.class);
        sslMode = getSetting(properties, SSL_MODE, "none", String.class);
        tenantId = getSetting(properties, TENANT_ID, "1", String.class);
        path = getSetting(properties, PATH, "/studio/fml/driver", String.class);
        keepAliveTimeout = getSetting(properties, KEEP_ALIVE_TIMEOUT, 60, Integer.class);
        connectionTimeout = getSetting(properties, CONNECTION_TIMEOUT, 60, Integer.class);
        timeToLiveMillis = getSetting(properties, TIME_TO_LIVE_MILLIS, 2000, Integer.class);
        sslRootCertificate = getSetting(properties, SSL_ROOT_CERTIFICATE, "", String.class);
        token = getSetting(properties, TOKEN, "", String.class);
        baseKey = getSetting(properties, BASE_KEY, "", String.class);
        buCode = getSetting(properties, BU_CODE, "", String.class);
    }

    @Override
    public Properties asProperty() {
        Properties properties = super.asProperty();
        properties.setProperty(PATH, path);
        properties.setProperty(KEEP_ALIVE_TIMEOUT, String.valueOf(keepAliveTimeout));
        properties.setProperty(CONNECTION_TIMEOUT, String.valueOf(connectionTimeout));
        properties.setProperty(SSL, String.valueOf(ssl));
        properties.setProperty(TOKEN, token);
        properties.setProperty(BASE_KEY, baseKey);
        properties.setProperty(TENANT_ID, tenantId);
        properties.setProperty(SSL_MODE, sslMode);
        properties.setProperty(BU_CODE, buCode);
        return properties;
    }
}
