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

package com.aliyun.fastmodel.driver.server.mybatis;

import com.aliyun.fastmodel.driver.server.DriverBaseTest;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/12/30
 */
public class ShowTablesMapperTest extends DriverBaseTest {

    @Test
    public void testMapper() {
        SqlSessionFactoryBuilder sqlSessionFactoryBuilder = new SqlSessionFactoryBuilder();
        SqlSessionFactory development = sqlSessionFactoryBuilder.build(
            ShowTablesMapperTest.class.getResourceAsStream("/mybatis/mybatis-config.xml"), "development");
        try (SqlSession session = development.openSession()) {
            ShowTablesMapper mapper = session.getMapper(ShowTablesMapper.class);
            ShowTables abc = mapper.showTables("abc");
            assertNotNull(abc);
            String name = abc.getName();
            assertNotNull(name);
        }
    }
}