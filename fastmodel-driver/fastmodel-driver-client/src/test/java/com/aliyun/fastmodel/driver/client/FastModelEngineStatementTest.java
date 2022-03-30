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

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;

import com.aliyun.fastmodel.driver.client.command.ExecuteCommand;
import com.aliyun.fastmodel.driver.client.command.tenant.TenantProperties;
import com.aliyun.fastmodel.driver.client.request.FastModelWrapperResponse;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/12/30
 */
@RunWith(MockitoJUnitRunner.class)
public class FastModelEngineStatementTest {

    FastModelEngineStatement fastModelEngineStatement;
    @Mock
    private FastModelEngineConnection connection;

    @Mock
    private TenantProperties properties;

    @Before
    public void before() throws SQLException {
        fastModelEngineStatement = new FastModelEngineStatement(connection, 0);
        ExecuteCommand executeCommand = Mockito.mock(ExecuteCommand.class);
        given(connection.getStrategy()).willReturn(executeCommand);
        FastModelWrapperResponse response = Mockito.mock(FastModelWrapperResponse.class);
        given(executeCommand.execute(anyString())).willReturn(response);
        String input = "{'success' : true}";
        InputStream inputStream = IOUtils.toInputStream(input, StandardCharsets.UTF_8);
        given(response.getResponse()).willReturn(inputStream);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testExecuteUpdateWithException() throws SQLException {
        fastModelEngineStatement.executeUpdate("select * from abc");
    }

    @Test
    public void testExecuteUpdate() throws SQLException, IOException {
        prepareConnection();
        given(properties.getHost()).willReturn("http://www.alibaba.com");
        int row = fastModelEngineStatement.executeUpdate("update a set b = 'col'");
        assertEquals(1, row);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testExecuteQueryWithException() throws SQLException {
        fastModelEngineStatement.executeQuery("update a set b = 'c'");
    }

    @Test
    public void testExecuteQuery() throws SQLException, IOException {
        prepareConnection();
        given(properties.getHost()).willReturn("http://www.alibaba.com");
        ResultSet resultSet = fastModelEngineStatement.executeQuery("select a from b;");
        assertNotNull(resultSet);
    }

    private void prepareConnection() throws IOException {
        CloseableHttpClient httpClient = Mockito.mock(CloseableHttpClient.class);

        CloseableHttpResponse closeableHttpResponse = Mockito.mock(CloseableHttpResponse.class);
        HttpEntity entity = Mockito.mock(HttpEntity.class);
        given(httpClient.execute(any(HttpPost.class))).willReturn(closeableHttpResponse);
        given(closeableHttpResponse.getEntity()).willReturn(entity);

        StatusLine statusLine = Mockito.mock(StatusLine.class);
        given(closeableHttpResponse.getStatusLine()).willReturn(statusLine);
        given(statusLine.getStatusCode()).willReturn(200);

        String input = "{'success' : true}";
        InputStream inputStream = IOUtils.toInputStream(input, StandardCharsets.UTF_8);
        given(entity.getContent()).willReturn(inputStream);
        given(properties.asProperty()).willReturn(new Properties());
    }
}
