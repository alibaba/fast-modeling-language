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

package com.aliyun.fastmodel.driver.cli.command;

import java.util.Properties;

import lombok.Getter;
import picocli.CommandLine.Option;

/**
 * 客户端可选配置
 *
 * @author panguanjing
 * @date 2020/12/22
 */
@Getter
public class ClientOptions {
    private static final String DEFAULT_VALUE = "(default: ${DEFAULT-VALUE})";
    @Option(names = {"--url", "-u"}, paramLabel = "<JDBC URL>", description = "JDBC URL" + DEFAULT_VALUE,
        descriptionKey = "url")
    public String server;

    @Option(names = {"--database", "-d"}, paramLabel = "<database>", description = "Project" + DEFAULT_VALUE,
        descriptionKey = "database")
    public String database;

    @Option(names = {"--access-token", "-ak"}, paramLabel = "<token>", description = "Access token",
        descriptionKey = "token")
    public String accessToken;

    @Option(names = {"--base-key", "-k"}, paramLabel = "<baseKey>", description = "Base Key",
        descriptionKey = "baseKey")
    public String baseKey;

    @Option(names = {"--path", "-p"}, paramLabel = "<path>", description = "Http Request Path", descriptionKey = "path")
    public String path;

    @Option(names = {"--tenant-id", "-t"}, paramLabel = "<tenantId>", description = "Tenant Id",
        descriptionKey = "tenantId")
    public String tenantId;

    @Option(names = "--ssl", paramLabel = "<ssl>", description = "Http Request Use SSL", descriptionKey = "ssl")
    public String ssl;

    @Option(names = "--user", paramLabel = "<user>", description = "User", descriptionKey = "user")
    public String user;

    @Option(names = "--show-name", paramLabel = "<showName>", description = "showName", descriptionKey = "showName")
    public String showName;

    @Option(names = "--bu-code", paramLabel = "<buCode>", description = "buCode", descriptionKey = "buCode")
    public String buCode;

    public Properties asProperties() {
        Properties properties = new Properties();
        if (ssl != null) {
            properties.setProperty("ssl", ssl);
        }
        if (server != null) {
            properties.setProperty("url", server);
        }
        if (path != null) {
            properties.setProperty("path", path);
        }
        if (baseKey != null) {
            properties.setProperty("baseKey", baseKey);
        }
        if (accessToken != null) {
            properties.setProperty("token", accessToken);
        }
        if (database != null) {
            properties.setProperty("database", database);
        }
        if (tenantId != null) {
            properties.setProperty("tenantId", tenantId);
        }
        if (user != null) {
            properties.setProperty("user", user);
        }
        if (showName != null) {
            properties.setProperty("showName", showName);
        }
        if (buCode != null) {
            properties.setProperty("buCode", buCode);
        }
        return properties;
    }
}
