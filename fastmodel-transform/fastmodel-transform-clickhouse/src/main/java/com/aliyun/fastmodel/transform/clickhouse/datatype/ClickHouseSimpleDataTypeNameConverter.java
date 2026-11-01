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

package com.aliyun.fastmodel.transform.clickhouse.datatype;

import com.aliyun.fastmodel.transform.api.datatype.simple.ISimpleDataTypeName;
import com.aliyun.fastmodel.transform.api.datatype.simple.SimpleDataTypeName;
import com.aliyun.fastmodel.transform.api.datatype.simple.SimpleDataTypeNameConverter;
import com.aliyun.fastmodel.transform.clickhouse.parser.tree.ClickHouseDataTypeName;
import com.google.auto.service.AutoService;

/**
 * clickHouse simple data type name converter
 *
 * @author panguanjing
 */
@AutoService(SimpleDataTypeNameConverter.class)
public class ClickHouseSimpleDataTypeNameConverter implements SimpleDataTypeNameConverter {

    @Override
    public SimpleDataTypeName convert(String dataTypeName) {
        ISimpleDataTypeName byValue = ClickHouseDataTypeName.getByValue(dataTypeName);
        return byValue.getSimpleDataTypeName();
    }
}
