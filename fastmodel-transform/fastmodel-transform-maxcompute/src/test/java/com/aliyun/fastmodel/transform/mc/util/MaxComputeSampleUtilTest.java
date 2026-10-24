/*
 * Copyright 2022 Alibaba Group Holding Ltd.
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

import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.datatype.NumericParameter;
import com.aliyun.fastmodel.core.tree.datatype.TypeParameter;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * MaxComputeSampleUtilTest
 *
 * @author panguanjing
 * @date 2022/5/25
 */
public class MaxComputeSampleUtilTest {

    @Test
    public void testSimpleSampleData() {
        BaseExpression baseExpression = MaxComputeSampleUtil.simpleSampleData(DataTypeUtil.simpleType(DataTypeEnums.BIGINT));
        assertEquals(baseExpression.toString(), "0");
    }

    @Test
    public void testSimpleDataVarchar() {
        BaseExpression baseExpression = MaxComputeSampleUtil.simpleSampleData(DataTypeUtil.simpleType(DataTypeEnums.VARCHAR, new NumericParameter("1")));
        assertEquals(baseExpression.toString(), "''");
    }

    @Test
    public void testSimpleDataChar() {
        BaseExpression baseExpression = MaxComputeSampleUtil.simpleSampleData(DataTypeUtil.simpleType(DataTypeEnums.CHAR, new NumericParameter("1")));
        assertEquals(baseExpression.toString(), "''");
    }

    @Test
    public void testSimpleDataString() {
        BaseExpression baseExpression = MaxComputeSampleUtil.simpleSampleData(DataTypeUtil.simpleType(DataTypeEnums.STRING));
        assertEquals(baseExpression.toString(), "''");
    }

    @Test
    public void testSimpleDataArray() {
        BaseExpression baseExpression = MaxComputeSampleUtil.simpleSampleData(
            DataTypeUtil.simpleType(DataTypeEnums.ARRAY, new TypeParameter(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))));
        assertEquals(baseExpression.toString(), "ARRAY(0)");
    }

    @Test
    public void testSimpleDataArrayNest() {
        BaseDataType type = DataTypeUtil.simpleType(DataTypeEnums.ARRAY, new TypeParameter(DataTypeUtil.simpleType(DataTypeEnums.STRING)));
        BaseExpression baseExpression = MaxComputeSampleUtil.simpleSampleData(
            DataTypeUtil.simpleType(DataTypeEnums.ARRAY, new TypeParameter(type)));
        assertEquals(baseExpression.toString(), "ARRAY(ARRAY(''))");
    }

    @Test
    public void testSimpleDataStruct() {
        BaseDataType type = DataTypeUtil.simpleType(DataTypeEnums.STRUCT, new TypeParameter(DataTypeUtil.simpleType(DataTypeEnums.STRING)),
            new TypeParameter(DataTypeUtil.simpleType(DataTypeEnums.BIGINT)));
        BaseExpression baseExpression = MaxComputeSampleUtil.simpleSampleData(type);
        assertEquals(baseExpression.toString(), "STRUCT('', 0)");
    }

    @Test
    public void testSimpleDataMap() {
        BaseDataType type = DataTypeUtil.simpleType(DataTypeEnums.MAP, new TypeParameter(DataTypeUtil.simpleType(DataTypeEnums.STRING)),
            new TypeParameter(DataTypeUtil.simpleType(DataTypeEnums.BIGINT)));
        BaseExpression baseExpression = MaxComputeSampleUtil.simpleSampleData(type);
        assertEquals(baseExpression.toString(), "MAP('', 0)");
    }

    @Test
    public void testSimpleDataDate() {
        BaseDataType type = DataTypeUtil.simpleType(DataTypeEnums.DATE);
        BaseExpression baseExpression = MaxComputeSampleUtil.simpleSampleData(type);
        assertTrue(baseExpression.toString().startsWith("GETDATE()"));
    }

    @Test
    public void testSimpleDataDateTime() {
        BaseDataType type = DataTypeUtil.simpleType(DataTypeEnums.DATETIME);
        BaseExpression baseExpression = MaxComputeSampleUtil.simpleSampleData(type);
        assertEquals(baseExpression.toString(), "GETDATE()");
    }

    @Test
    public void testSimpleDataTimestamp() {
        BaseDataType type = DataTypeUtil.simpleType(DataTypeEnums.TIMESTAMP);
        BaseExpression baseExpression = MaxComputeSampleUtil.simpleSampleData(type);
        assertTrue(baseExpression.toString().startsWith("GETDATE()"));
    }
}
