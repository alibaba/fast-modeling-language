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

package com.aliyun.fastmodel.driver.client.command;

import java.util.Properties;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.BooleanUtils;

/**
 * base command properties define
 *
 * @author panguanjing
 * @date 2021/3/25
 */
@Getter
@Setter
public abstract class BaseCommandProperties {

    private String database;
    private String user;
    private String password;
    private String host;
    private Integer port;

    public BaseCommandProperties(Properties properties) {
        host = getSetting(properties, "host", "", String.class);
        port = getSetting(properties, "port", 0, Integer.class);
        database = getSetting(properties, "database", "", String.class);
        user = getSetting(properties, "user", "", String.class);
        password = getSetting(properties, "password", "", String.class);
    }

    public <T> T getSetting(Properties info, String key, Object defaultValue, Class<T> clazz) {
        if (info == null) {
            return (T)defaultValue;
        }
        String value = info.getProperty(key);
        if (value == null) {
            return (T)defaultValue;
        }
        if (clazz == int.class || clazz == Integer.class) {
            return (T)clazz.cast(Integer.parseInt(value));
        }
        if (clazz == long.class || clazz == Long.class) {
            return clazz.cast(Long.parseLong(value));
        }
        if (clazz == boolean.class || clazz == Boolean.class) {
            return clazz.cast(BooleanUtils.toBoolean(value));
        }
        return (T)clazz.cast(value);
    }

    public Properties asProperty() {
        Properties properties = new Properties();
        properties.setProperty("database", database);
        properties.setProperty("user", user);
        properties.setProperty("password", password);
        properties.setProperty("host", host);
        properties.setProperty("port", String.valueOf(port));
        return properties;
    }

}
