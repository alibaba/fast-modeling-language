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
import java.io.Reader;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import com.aliyun.fastmodel.driver.client.exception.FastModelException;
import com.aliyun.fastmodel.driver.client.metadata.FastModelResultSetMetaData;
import com.aliyun.fastmodel.driver.model.DriverColumnInfo;
import com.aliyun.fastmodel.driver.model.DriverResult;
import com.aliyun.fastmodel.driver.model.DriverRow;
import com.aliyun.fastmodel.driver.model.QueryResult;
import lombok.Getter;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.SerializationUtils;

/**
 * ResultSet
 *
 * @author panguanjing
 * @date 2020/12/4
 */
@Getter
public class FastModelResultSet implements ResultSet {

    private final JSONObject is;

    private final FastModelEngineStatement fastModelEngineStatement;

    private List<DriverRow> rows;

    private DriverRow nextLine;

    private DriverRow values;

    private List<DriverColumnInfo> driverColumnInfoList;

    private int currentIndex;

    private int lastReadColumn;

    private static final String DATE_PATTERN = "yyyy-MM-dd";
    private static final String DATE_TIME_PATTERN = "yyyy-MM-dd HH:mm:ss";
    private static final String ZERO_TIMESTAMP = "0000-00-00 00:00:00";

    private boolean isAfterLastReached;
    private boolean lastReached = false;

    public FastModelResultSet(JSONObject jsonObject, FastModelEngineStatement fastModelEngineStatement)
        throws FastModelException {
        is = jsonObject;
        this.fastModelEngineStatement = fastModelEngineStatement;
        init(jsonObject);
    }

    private void init(JSONObject json) throws FastModelException {
        JSONObject data = json.getJSONObject("data");
        if (data == null) {
            return;
        }
        DriverResult driverResult = data.toJavaObject(DriverResult.class);
        if (!driverResult.isSuccess()) {
            throw new FastModelException(
                "execute error, reason:" + driverResult.getErrorMessage(),
                null,
                fastModelEngineStatement.getProperties().getHost()
            );
        }
        JSONObject queryResult = (JSONObject)driverResult.getData();
        if (queryResult == null) {
            return;
        }
        JSONArray columnInfos = queryResult.getJSONArray("columnInfos");
        if (columnInfos == null) {
            return;
        }
        JSONArray rows = queryResult.getJSONArray("rows");
        if (rows == null) {
            rows = new JSONArray();
        }
        QueryResult result = new QueryResult(
            columnInfos.toJavaList(DriverColumnInfo.class),
            rows.toJavaList(DriverRow.class)
        );
        this.rows = result.getRows();
        driverColumnInfoList = result.getColumnInfos();
    }

    @Override
    public boolean next() throws SQLException {
        boolean hasNext = hasNext();
        if (hasNext) {
            values = rows.get(currentIndex);
            nextLine = null;
            currentIndex += 1;
            return true;
        }
        isAfterLastReached = true;
        return false;
    }

    private boolean hasNext() throws SQLException {
        if (rows == null) {
            return false;
        }
        if (nextLine == null && !lastReached) {
            boolean end = currentIndex == rows.size();
            if (end) {
                try {
                    endStream();
                } catch (IOException e) {
                    throw new SQLException(e);
                }
            } else {
                nextLine = rows.get(currentIndex);
                if (nextLine == null) {
                    try {
                        endStream();
                    } catch (IOException e) {
                        throw new SQLException(e);
                    }
                }
            }
        }
        return nextLine != null;
    }

    private void endStream() throws IOException {
        lastReached = true;
        nextLine = null;
    }

    @Override
    public void close() throws SQLException {
    }

    @Override
    public boolean wasNull() throws SQLException {
        if (lastReadColumn == 0) {
            throw new IllegalStateException("You should get something before check nullability");
        }
        return getValue(lastReadColumn) == null;
    }

    private Object getValue(int columnIndex) {
        lastReadColumn = columnIndex;
        return values.getValue(columnIndex - 1);
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        return toString(getValue(columnIndex));
    }

    /**
     * toString
     *
     * @param value
     * @return
     */
    private String toString(Object value) {
        return value.toString();
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        return toBoolean(getValue(columnIndex));
    }

    private boolean toBoolean(Object value) {
        return BooleanUtils.toBoolean(value.toString());
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        return Byte.parseByte(toString(getValue(columnIndex)));
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        return Short.parseShort(toString(getValue(columnIndex)));
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return Integer.parseInt(toString(getValue(columnIndex)));
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return Long.parseLong(toString(getValue(columnIndex)));
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return Float.parseFloat(toString(getValue(columnIndex)));
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return Double.parseDouble(toString(getValue(columnIndex)));
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        Object object = getValue(columnIndex);
        if (object == null) {
            return null;
        }
        BigDecimal bigDecimal = new BigDecimal(object.toString());
        return bigDecimal.setScale(scale, RoundingMode.HALF_UP);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        return SerializationUtils.serialize((Serializable)getValue(columnIndex));
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(DATE_PATTERN);
        try {
            return new Date(simpleDateFormat.parse(toString(getValue(columnIndex))).getTime());
        } catch (ParseException e) {
            return null;
        }
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        Timestamp ts = getTimestamp(columnIndex);
        if (ts == null) {
            return null;
        }
        return new Time(ts.getTime());
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        Long value = getTimestampAsLong(columnIndex);
        return value == null ? null : new Timestamp(value.longValue());
    }

    private Long getTimestampAsLong(int columnIndex) {
        Object value = getValue(columnIndex);
        String stringValue = toString(value);
        if (ZERO_TIMESTAMP.equals(stringValue)) {
            return null;
        }
        try {
            return new SimpleDateFormat(DATE_TIME_PATTERN).parse(stringValue).getTime();
        } catch (ParseException e) {
            return null;
        }
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        Integer colIndex = getColNum(columnLabel);
        return getString(colIndex);
    }

    private Integer getColNum(String columnLabel) {
        for (int i = 0; i < driverColumnInfoList.size(); i++) {
            if (columnLabel.equals(driverColumnInfoList.get(i).getColumnName())) {
                return i + 1;
            }
        }
        throw new RuntimeException("no column " + columnLabel + " in columns List ");
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        Integer index = getColNum(columnLabel);
        return getBoolean(index);
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        Integer index = getColNum(columnLabel);
        return getByte(index);
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        Integer index = getColNum(columnLabel);
        return getShort(index);
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        Integer index = getColNum(columnLabel);
        return getInt(index);
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        Integer index = getColNum(columnLabel);
        return getLong(index);
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        Integer index = getColNum(columnLabel);
        return getFloat(index);
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        Integer index = getColNum(columnLabel);
        return getDouble(index);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        Integer index = getColNum(columnLabel);
        return getBigDecimal(index, scale);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        Integer index = getColNum(columnLabel);
        return getBytes(index);
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        Integer index = getColNum(columnLabel);
        return getDate(index);
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        Integer index = getColNum(columnLabel);
        return getTime(index);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        Integer index = getColNum(columnLabel);
        return getTimestamp(index);
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        Integer index = getColNum(columnLabel);
        return getAsciiStream(index);
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        Integer index = getColNum(columnLabel);
        return getUnicodeStream(index);
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        Integer index = getColNum(columnLabel);
        return getBinaryStream(index);
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clearWarnings() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getCursorName() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return new FastModelResultSetMetaData(this);
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return getValue(columnIndex);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        Integer columnIndex = getColNum(columnLabel);
        return getObject(columnIndex);
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        return getColNum(columnLabel);
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        String string = getString(columnIndex);
        if (string == null) {
            return null;
        }
        return new BigDecimal(string);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        Integer index = getColNum(columnLabel);
        return getBigDecimal(index);
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return currentIndex == 0 && hasNext();
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return isAfterLastReached;
    }

    @Override
    public boolean isFirst() throws SQLException {
        return currentIndex == 1;
    }

    @Override
    public boolean isLast() throws SQLException {
        return !hasNext();
    }

    @Override
    public void beforeFirst() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void afterLast() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean first() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean last() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getRow() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean previous() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getFetchDirection() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getFetchSize() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getType() throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean rowInserted() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {

        throw new UnsupportedOperationException();

    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {

    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {

    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {

    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {

    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {

    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {

    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {

    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {

    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {

    }

    @Override
    public void insertRow() throws SQLException {

    }

    @Override
    public void updateRow() throws SQLException {

    }

    @Override
    public void deleteRow() throws SQLException {

    }

    @Override
    public void refreshRow() throws SQLException {

    }

    @Override
    public void cancelRowUpdates() throws SQLException {

    }

    @Override
    public void moveToInsertRow() throws SQLException {

    }

    @Override
    public void moveToCurrentRow() throws SQLException {

    }

    @Override
    public Statement getStatement() throws SQLException {
        return fastModelEngineStatement;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return null;
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return null;
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {

    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {

    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {

    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {

    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {

    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {

    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {

    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {

    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {

    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {

    }

    @Override
    public int getHoldability() throws SQLException {
        return 0;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return false;
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {

    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {

    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {

    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new UnsupportedOperationException();

    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException();

    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new UnsupportedOperationException();

    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new UnsupportedOperationException();

    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new UnsupportedOperationException();

    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {

        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {

        throw new UnsupportedOperationException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {

        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {

        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        Object object = getObject(columnIndex);
        if (object == null) {
            return null;
        }
        return type.cast(object);
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return getObject(getColNum(columnLabel), type);
    }

    @Override
    public <T> T unwrap(Class<T> face) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isWrapperFor(Class<?> face) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public String getDb() {
        return fastModelEngineStatement.getProperties().getDatabase();
    }
}
