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

package com.aliyun.fastmodel.driver.server.command;

import java.util.List;

import com.aliyun.fastmodel.core.tree.statement.show.ShowObjects;
import com.aliyun.fastmodel.driver.model.DriverColumnInfo;
import com.aliyun.fastmodel.driver.model.DriverDataType;
import com.aliyun.fastmodel.driver.model.DriverResult;
import com.aliyun.fastmodel.driver.model.DriverRow;
import com.aliyun.fastmodel.driver.model.QueryResult;
import com.google.common.collect.ImmutableList;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/12/9
 */
@Command(ShowObjects.class)
public class ShowObjectCommand implements FmlCommand<ShowObjects, QueryResult> {
    @Override
    public DriverResult<QueryResult> execute(ShowObjects params) {
        List<DriverColumnInfo> list = toColumnInfo();
        List<DriverRow> rows = toRow();
        QueryResult queryResult = new QueryResult(
            list,
            rows
        );
        return new DriverResult<>(queryResult, true, null);
    }

    private List<DriverRow> toRow() {
        return ImmutableList.of(new DriverRow(ImmutableList.of("abc", "bcd")));
    }

    private List<DriverColumnInfo> toColumnInfo() {
        return ImmutableList.of(new DriverColumnInfo("name", new DriverDataType(String.class)),
            new DriverColumnInfo("comment", new DriverDataType(String.class)));
    }
}
