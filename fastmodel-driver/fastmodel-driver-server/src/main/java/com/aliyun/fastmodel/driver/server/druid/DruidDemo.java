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

package com.aliyun.fastmodel.driver.server.druid;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import com.alibaba.druid.pool.DruidDataSource;

/**
 * 使用Druid连接池进行使用
 *
 * @author panguanjing
 * @date 2021/1/9
 */
public class DruidDemo {

    DruidDataSource druidDataSource = new DruidDataSource();

    public DruidDemo() {
    }

    public void execute(String sql) throws SQLException {
        try (Connection connection = druidDataSource.getConnection()) {
            Statement statement = connection.createStatement();
            statement.execute(sql);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    public void init(String url, String driverClass, Properties properties) throws SQLException {
        druidDataSource.setUrl(url);
        druidDataSource.setDriverClassName(driverClass);
        druidDataSource.setConnectProperties(properties);
        druidDataSource.init();
    }

    public void close() {
        druidDataSource.close();
    }
}
