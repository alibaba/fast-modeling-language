/*
 * Copyright 2021 Alibaba Group Holding Ltd.
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

package com.aliyun.fastmodel.transform.mc.util;

import java.util.Locale;

import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;

/**
 * MaxComputeV1DataTypeUtil
 *
 * @author panguanjing
 * @date 2021/6/1
 */
public class MaxComputeV1DataTypeUtil {

    /**
     * 如果是varchar或者char，统一转换为string处理。
     * 如果不需要转换，返回空
     *
     * @param typeName
     * @return DataTypeEnums
     */
    public static IDataTypeName convertIfNecessary(IDataTypeName typeName) {
        if (typeName == null || typeName.getValue() == null) {
            return null;
        }
        switch (typeName.getValue().toUpperCase(Locale.ROOT)) {
            case "VARCHAR":
            case "CHAR":
            case "TEXT":
                return DataTypeEnums.STRING;
            case "TINYINT":
            case "SMALLINT":
            case "INT":
                return DataTypeEnums.BIGINT;
            case "FLOAT":
                return DataTypeEnums.DOUBLE;
            case "DATE":
            case "TIMESTAMP":
                return DataTypeEnums.DATETIME;
            default:
                return null;
        }
    }
}
