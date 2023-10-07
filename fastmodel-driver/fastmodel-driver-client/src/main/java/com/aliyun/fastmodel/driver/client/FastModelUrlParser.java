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

package com.aliyun.fastmodel.driver.client;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.aliyun.fastmodel.driver.client.exception.IllegalConfigException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/12/3
 */
@Slf4j
public class FastModelUrlParser {

    public static final String JDBC_PREFIX = "jdbc:";
    public static final String JDBC_FASTMODEL_PREFIX = JDBC_PREFIX + "fastmodel:";
    public static final Pattern DB_PATH_PATTERN = Pattern.compile("/([a-zA-Z0-9_*\\-]+)");
    public static final String DATABASE = "database";
    public static final String SPLASH = "/";
    public static final String SUFFIX = ":";
    public static final String SEPARATOR = "//";

    public static Properties parse(String jdbcUrl, Properties defaults) throws URISyntaxException {
        int index = jdbcUrl.indexOf(SEPARATOR);
        return parseFastModelUrl(jdbcUrl.substring(index), defaults);
    }

    public static String getMode(String url) {
        int startIndex = url.indexOf(JDBC_FASTMODEL_PREFIX);
        int endIndex = url.indexOf(SEPARATOR);
        String substring = url.substring(startIndex + JDBC_FASTMODEL_PREFIX.length(), endIndex);
        if (substring.endsWith(SUFFIX)) {
            return substring.substring(0, substring.indexOf(SUFFIX));
        }
        return substring;
    }

    private static Properties parseFastModelUrl(String uriString, Properties defaults)
        throws URISyntaxException {
        URI uri = new URI(uriString);
        Properties urlProperties = parseUriQueryPart(uri.getQuery(), defaults);
        if (StringUtils.isBlank(uri.getHost())) {
            throw new IllegalConfigException("url is not valid", null, uriString);
        }
        urlProperties.setProperty("host", uri.getHost());
        if (uri.getPort() > 0) {
            urlProperties.setProperty("port", String.valueOf(uri.getPort()));
        }
        String path = uri.getPath();
        String database;
        if (path == null || path.isEmpty() || path.equals(SPLASH)) {
            database = defaults.getProperty(DATABASE);
        } else {
            Matcher m = DB_PATH_PATTERN.matcher(path);
            if (m.matches()) {
                database = m.group(1);
            } else {
                throw new URISyntaxException("wrong database name path:" + path, uriString);
            }
        }
        if (StringUtils.isNotBlank(database)) {
            urlProperties.setProperty(DATABASE, database);
        }
        return urlProperties;
    }

    public static Properties parseUriQueryPart(String query, Properties defaults) {
        if (query == null) {
            return defaults;
        }
        Properties urlProps = new Properties(defaults);
        String[] queryKeyValues = query.split("&");
        for (String keyValue : queryKeyValues) {
            String[] keyValueTokens = keyValue.split("=");
            if (keyValueTokens.length == 2) {
                urlProps.put(keyValueTokens[0], keyValueTokens[1]);
            } else {
                log.warn("don't know how to handle parameter pair: {}", keyValue);
            }
        }
        return urlProps;
    }
}

