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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import lombok.Getter;

/**
 * 用于预编译的statement
 *
 * @author panguanjing
 * @date 2020/12/3
 */
@Getter
public class FastModelEnginePrepareStatement extends FastModelEngineStatement implements PreparedStatement {
    /**
     * param step
     */
    public static final int PARAM_STEP = 2;
    /**
     * 传入执行的sql
     */
    private String sql;

    private final Object[] sqlAndParams;

    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

    public FastModelEnginePrepareStatement(FastModelEngineConnection fastModelEngineConnection,
                                           String sql,
                                           int resultSetType) {
        super(fastModelEngineConnection, resultSetType);
        this.sql = sql.trim();
        //获取到参数问号
        String[] parts = (sql + ";").split("\\?");
        sqlAndParams = new Object[parts.length * 2 - 1];
        for (int i = 0; i < parts.length; i++) {
            sqlAndParams[i * 2] = parts[i];
        }
    }

    public String buildSql() throws SQLException {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < sqlAndParams.length; i++) {
            try {
                if (sqlAndParams[i] instanceof Date) {
                    sb.append("'" + dateFormat.format((Date)sqlAndParams[i]) + "' ");
                } else {
                    sb.append(sqlAndParams[i] + " ");
                }
            } catch (Exception e) {
                throw new SQLException(
                    "Unable to create SQL statement, [" + i + "] = " + sqlAndParams[i] + " : " + e.getMessage(), e);
            }
        }
        return sb.substring(0, sb.length() - 2);
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        if (super.execute(buildSql())) {
            return getResultSet();
        }
        return null;
    }

    @Override
    public int executeUpdate() throws SQLException {
        return super.executeUpdate(buildSql());
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        sqlAndParams[(parameterIndex * 2) - 1] = null;
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        sqlAndParams[(parameterIndex * 2) - 1] = Boolean.valueOf(x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        sqlAndParams[(parameterIndex * 2) - 1] = Byte.valueOf(x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        sqlAndParams[(parameterIndex * 2) - 1] = Short.valueOf(x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        sqlAndParams[(parameterIndex * 2) - 1] = Integer.valueOf(x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        sqlAndParams[(parameterIndex * 2) - 1] = Long.valueOf(x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        sqlAndParams[(parameterIndex * 2) - 1] = Float.valueOf(x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        sqlAndParams[(parameterIndex * 2) - 1] = Double.valueOf(x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        sqlAndParams[(parameterIndex * 2) - 1] = x;
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        sqlAndParams[(parameterIndex * 2) - 1] = "'" + x + "'";
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        sqlAndParams[(parameterIndex * 2) - 1] = x;
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        sqlAndParams[(parameterIndex * 2) - 1] = x;
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        sqlAndParams[(parameterIndex * 2) - 1] = Long.valueOf(x.getTime());
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        sqlAndParams[(parameterIndex * 2) - 1] = Long.valueOf(x.getTime());
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException(getLoggingInfo());
    }

    public static String getLoggingInfo() {
        StackTraceElement element = Thread.currentThread().getStackTrace()[2];
        return element.getClassName() + "." + element.getMethodName() + " [" + element.getLineNumber() + "]";
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException(getLoggingInfo());
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException(getLoggingInfo());
    }

    @Override
    public void clearParameters() throws SQLException {
        for (int i = 1; i < sqlAndParams.length; i += PARAM_STEP) {
            sqlAndParams[i] = null;
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        if (x instanceof String) {
            sqlAndParams[(parameterIndex * 2) - 1] = "'" + x + "'";
        } else {
            sqlAndParams[(parameterIndex * 2) - 1] = x;
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        if (x instanceof String) {
            sqlAndParams[(parameterIndex * 2) - 1] = "'" + x + "'";
        } else {
            sqlAndParams[(parameterIndex * 2) - 1] = x;
        }
    }

    @Override
    public boolean execute() throws SQLException {
        return super.execute(buildSql());
    }

    @Override
    public void addBatch() throws SQLException {
        super.addBatch(buildSql());
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException(getLoggingInfo());
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException(getLoggingInfo());
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException(getLoggingInfo());
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException(getLoggingInfo());
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        sqlAndParams[(parameterIndex * 2) - 1] = x;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return getCurrentResult().getMetaData();
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException(getLoggingInfo());
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException(getLoggingInfo());
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException(getLoggingInfo());
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        sqlAndParams[(parameterIndex * 2) - 1] = null;
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        sqlAndParams[(parameterIndex * 2) - 1] = x;
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException(getLoggingInfo());
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException(getLoggingInfo());
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        sqlAndParams[(parameterIndex * 2) - 1] = value;
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(getLoggingInfo());
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new SQLFeatureNotSupportedException(getLoggingInfo());
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(getLoggingInfo());
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(getLoggingInfo());
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(getLoggingInfo());
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException(getLoggingInfo());
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException(getLoggingInfo());
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(getLoggingInfo());
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(getLoggingInfo());
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException(getLoggingInfo());
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException(getLoggingInfo());
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException(getLoggingInfo());
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException(getLoggingInfo());
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException(getLoggingInfo());
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException(getLoggingInfo());
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException(getLoggingInfo());
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException(getLoggingInfo());
    }
}
