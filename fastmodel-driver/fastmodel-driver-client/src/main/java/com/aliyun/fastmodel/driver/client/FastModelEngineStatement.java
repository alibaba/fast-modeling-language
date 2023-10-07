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
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.aliyun.fastmodel.driver.client.command.BaseCommandProperties;
import com.aliyun.fastmodel.driver.client.command.ExecuteCommand;
import com.aliyun.fastmodel.driver.client.exception.FastModelException;
import com.aliyun.fastmodel.driver.client.exception.ResultContainErrorException;
import com.aliyun.fastmodel.driver.client.exception.ResultParseException;
import com.aliyun.fastmodel.driver.client.request.FastModelWrapperResponse;
import com.aliyun.fastmodel.driver.model.DriverUtil;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.BooleanUtils;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/12/3
 */
@Getter
@Slf4j
public class FastModelEngineStatement implements Statement {

    private final FastModelEngineConnection connection;

    private final boolean isResultSetScrollable;

    private boolean closeOnCompletion;

    private int maxRows;
    private int queryTimeout;
    private ResultSet currentResult;
    private int currentUpdateCount;

    public static final String REQUEST_ID = "requestId";

    public static final String SQL = "sql";

    public FastModelEngineStatement(FastModelEngineConnection fastModelEngineConnection,
                                    int resultSetType) {
        connection = fastModelEngineConnection;
        isResultSetScrollable = resultSetType != ResultSet.TYPE_FORWARD_ONLY;

    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        boolean select = isSelect(sql);
        if (!select) {
            throw new SQLFeatureNotSupportedException("Provided query is not a SELECT, SHOW, DESC, CALL");
        }
        ExecuteCommand strategy = connection.getStrategy();
        FastModelWrapperResponse response = strategy.execute(sql);
        InputStream is = null;
        try {
            JSONObject check = check(response);
            is = response.getResponse();
            currentResult = new FastModelResultSet(check, this);
            return currentResult;
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    log.error("can not close stream", e);
                }
            }
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        if (isSelect(sql)) {
            throw new SQLFeatureNotSupportedException("Unable to parse provided update sql");
        }
        InputStream is = null;
        try {
            FastModelWrapperResponse response = connection.getStrategy().execute(sql);
            is = response.getResponse();
            check(response);
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    log.error("can not close stream", e);
                }
            }
        }
        return 1;
    }

    private String toStringValue(InputStream is) throws IOException {
        return IOUtils.toString(is, StandardCharsets.UTF_8);
    }

    private JSONObject check(FastModelWrapperResponse response) throws FastModelException {
        InputStream is = response.getResponse();
        JSONObject json;
        try {
            json = JSON.parseObject(toStringValue(is));
        } catch (IOException e) {
            throw new ResultParseException(e.getMessage(), connection.getCommandProperties().getHost())
                .addContextValue(SQL, response.getRequestSql()).addContextValue(REQUEST_ID, response.getRequestId());
        }
        if (json == null) {
            throw new ResultParseException("response is null", connection.getCommandProperties().getHost()
            ).addContextValue(SQL, response.getRequestSql()).addContextValue(REQUEST_ID,
                response.getRequestId());
        }
        Boolean success = json.getBoolean("success");
        if (BooleanUtils.isTrue(success)) {
            return json;
        }
        boolean status = json.containsKey("status");
        String errorCode;
        if (status) {
            errorCode = json.getString("status");
        } else {
            errorCode = json.getString("errCode");
        }
        boolean message = json.containsKey("message");
        String errorMessage;
        if (message) {
            errorMessage = json.getString("message");
        } else {
            errorMessage = json.getString("errMsg");
        }
        throw new ResultContainErrorException(
            errorCode + ":" + errorMessage,
            getProperties().getHost()
        ).addContextValue(SQL, response.getRequestSql()).addContextValue(REQUEST_ID,
            response.getRequestId());
    }

    public BaseCommandProperties getProperties() {
        return connection.getCommandProperties();
    }

    @Override
    public void close() throws SQLException {
        if (currentResult != null) {
            currentResult.close();
        }
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {

    }

    @Override
    public int getMaxRows() throws SQLException {
        return maxRows;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        if (max < 0) {
            throw new SQLException("Illegal maxRows value:" + max);
        }
        maxRows = max;
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {

    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return queryTimeout;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        queryTimeout = seconds;
    }

    @Override
    public void cancel() throws SQLException {

    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public void setCursorName(String name) throws SQLException {

    }

    @Override
    public boolean execute(String sql) throws SQLException {
        if (isSelect(sql)) {
            ResultSet resultSet = executeQuery(sql);
            return resultSet != null;
        }
        executeUpdate(sql);
        return false;
    }

    public static boolean isSelect(String sql) {
        return DriverUtil.isSelect(sql);
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return currentResult;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return currentUpdateCount;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        if (currentResult != null) {
            currentResult.close();
            currentResult = null;
        }
        currentUpdateCount = -1;
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {

    }

    @Override
    public int getFetchDirection() throws SQLException {
        return 0;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {

    }

    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return 0;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return currentResult.getType();
    }

    @Override
    public void addBatch(String sql) throws SQLException {

    }

    @Override
    public void clearBatch() throws SQLException {

    }

    @Override
    public int[] executeBatch() throws SQLException {
        return new int[0];
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return null;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return 0;
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return 0;
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return 0;
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return false;
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return false;
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return false;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return 0;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return false;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {

    }

    @Override
    public boolean isPoolable() throws SQLException {
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        closeOnCompletion = true;
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return closeOnCompletion;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        throw new SQLException("cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }
}
