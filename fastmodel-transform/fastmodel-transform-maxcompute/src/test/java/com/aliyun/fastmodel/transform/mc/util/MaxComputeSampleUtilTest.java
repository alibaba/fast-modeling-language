/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
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
