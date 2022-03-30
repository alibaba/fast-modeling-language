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

import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

import com.aliyun.fastmodel.driver.client.command.CommandFactory;
import com.aliyun.fastmodel.driver.client.command.CommandType;
import com.aliyun.fastmodel.driver.client.command.ExecuteCommand;
import com.aliyun.fastmodel.driver.client.exception.IllegalConfigException;
import com.aliyun.fastmodel.driver.client.utils.Version;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import static com.aliyun.fastmodel.driver.client.FastModelUrlParser.JDBC_FASTMODEL_PREFIX;

/**
 * 用于model-engine的驱动器.
 *
 * @author panguanjing
 * @date 2020/12/3
 */
@Slf4j
public class FastModelEngineDriver implements Driver {

    static {
        FastModelEngineDriver driver = new FastModelEngineDriver();
        try {
            DriverManager.registerDriver(driver);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        if (log.isDebugEnabled()) {
            log.debug("Driver registered");
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            throw new IllegalConfigException("unSupported the url", null, url);
        }
        String mode = FastModelUrlParser.getMode(url);
        try {
            Properties properties = FastModelUrlParser.parse(url, info == null ? new Properties() : info);
            ExecuteCommand command = null;
            if (StringUtils.isBlank(mode)) {
                command = CommandFactory.createStrategy(CommandType.TENANT, properties);
            } else {
                CommandType commandType = CommandType.valueOf(mode.toUpperCase());
                command = CommandFactory.createStrategy(commandType, properties);
            }
            return new FastModelEngineConnection(url, command);
        } catch (URISyntaxException e) {
            throw new IllegalConfigException("properties is illegal", e, url).addContextValue(
                "info", info
            );
        }
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url.startsWith(JDBC_FASTMODEL_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        Properties copy = new Properties(info);
        Properties properties = FastModelUrlParser.parseUriQueryPart(url, copy);
        List<DriverPropertyInfo> list = new ArrayList<>(dumpProperty(properties));
        return list.toArray(new DriverPropertyInfo[0]);
    }

    private List<DriverPropertyInfo> dumpProperty(Properties properties) {
        List<DriverPropertyInfo> list = new ArrayList<>(properties.size());
        for (Object key : properties.keySet()) {
            String name = key.toString();
            DriverPropertyInfo driverPropertyInfo = new DriverPropertyInfo(name,
                properties.getProperty(name)
            );
            list.add(driverPropertyInfo);
        }
        return list;
    }

    @Override
    public int getMajorVersion() {
        return Version.majorVersion;
    }

    @Override
    public int getMinorVersion() {
        return Version.minorVersion;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }
}
