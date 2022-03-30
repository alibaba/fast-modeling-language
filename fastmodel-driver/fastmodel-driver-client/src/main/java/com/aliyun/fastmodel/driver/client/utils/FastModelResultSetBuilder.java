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

package com.aliyun.fastmodel.driver.client.utils;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.fastjson.JSONObject;

import com.aliyun.fastmodel.driver.client.FastModelResultSet;
import com.aliyun.fastmodel.driver.client.exception.FastModelException;
import com.aliyun.fastmodel.driver.model.DriverColumnInfo;
import com.aliyun.fastmodel.driver.model.DriverDataType;
import com.aliyun.fastmodel.driver.model.DriverResult;
import com.aliyun.fastmodel.driver.model.DriverRow;
import com.aliyun.fastmodel.driver.model.QueryResult;
import lombok.Builder;
import lombok.Getter;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/1/22
 */
@Builder
@Getter
public class FastModelResultSetBuilder {

    private List<String> names;
    private List<String> types;
    private List<List<?>> rows = new ArrayList<List<?>>();

    public FastModelResultSet emptyResultSet() throws FastModelException {
        JSONObject jsonObject = new JSONObject();
        DriverResult<QueryResult> value = new DriverResult<>();
        List<DriverColumnInfo> columnInfo = new ArrayList<>();
        columnInfo.add(new DriverColumnInfo("some", new DriverDataType(String.class)));
        QueryResult queryResult = new QueryResult(columnInfo, new ArrayList<>());
        value.setData(queryResult);
        value.setSuccess(true);
        jsonObject.put("data", value);
        return new FastModelResultSet(jsonObject, null);
    }

    public FastModelResultSet createResult() throws FastModelException {
        DriverResult<QueryResult> driverResult = new DriverResult<>();
        List<DriverColumnInfo> columnInfo = toDriverColumn();
        List<DriverRow> rowValues = toDriverRow();
        QueryResult queryResult = new QueryResult(
            columnInfo,
            rowValues
        );
        driverResult.setSuccess(true);
        driverResult.setData(queryResult);

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("data", driverResult);
        return new FastModelResultSet(jsonObject, null);
    }

    private List<DriverRow> toDriverRow() {
        List<List<?>> rows = this.rows;
        List<DriverRow> driverRows = new ArrayList<>();
        for (List<?> row : rows) {
            driverRows.add(new DriverRow(row));
        }
        return driverRows;
    }

    private List<DriverColumnInfo> toDriverColumn() {
        List<String> names = getNames();
        List<String> type = getTypes();
        if (names.size() != types.size()) {
            throw new IllegalArgumentException("names size must equal types size");
        }
        List<DriverColumnInfo> driverColumnInfoList = new ArrayList<>();
        for (int i = 0; i < names.size(); i++) {
            try {
                DriverColumnInfo driverColumnInfo = new DriverColumnInfo(
                    names.get(i),
                    new DriverDataType(Class.forName(type.get(i)))
                );
                driverColumnInfoList.add(driverColumnInfo);
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException("can't find the type with className:" + type.get(i));
            }
        }
        return driverColumnInfoList;
    }

}
