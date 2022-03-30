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

package com.aliyun.fastmodel.transform.mysql.datatype;

import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.NumericParameter;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Mysql2OracleDataTypeTransformerTest
 *
 * @author panguanjing
 * @date 2021/8/16
 */
public class Mysql2OracleDataTypeConverterTest {

    Mysql2OracleDataTypeConverter mysql2OracleDataTypeTransformer = new Mysql2OracleDataTypeConverter();

    @Test
    public void convert() {
        BaseDataType baseDataType = DataTypeUtil.simpleType("INT1", ImmutableList.of());
        assertDataType(baseDataType, DataTypeUtil.simpleType("NUMBER", ImmutableList.of(new NumericParameter("3"))));
    }

    private void assertDataType(BaseDataType source, BaseDataType assertDataType) {
        BaseDataType convert = mysql2OracleDataTypeTransformer.convert(source);
        assertEquals(convert, assertDataType);
    }

    @Test
    public void getSourceDialect() {
        DialectMeta sourceDialect = mysql2OracleDataTypeTransformer.getSourceDialect();
        assertEquals(sourceDialect.getName(), DialectMeta.DEFAULT_MYSQL.getName());
    }

    @Test
    public void getTargetDialect() {
        DialectMeta targetDialect = mysql2OracleDataTypeTransformer.getTargetDialect();
        assertEquals(targetDialect.getName(), DialectName.ORACLE);
    }
}