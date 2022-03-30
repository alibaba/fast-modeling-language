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

package com.aliyun.fastmodel.transform.oracle.datatype;

import java.util.List;

import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeParameter;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.datatype.NumericParameter;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Oracle2MysqlDataTypeTransformerTest
 *
 * @author panguanjing
 * @date 2021/8/14
 */
public class Oracle2MysqlDataTypeConverterTest {

    Oracle2MysqlDataTypeConverter oracle2MysqlDataTypeTransformer = new Oracle2MysqlDataTypeConverter();

    @Test
    public void testSource() {
        DialectMeta sourceDialect = oracle2MysqlDataTypeTransformer.getSourceDialect();
        assertEquals(sourceDialect, DialectMeta.getByName(DialectName.ORACLE));
    }

    @Test
    public void testDest() {
        DialectMeta targetMeta = oracle2MysqlDataTypeTransformer.getTargetDialect();
        assertEquals(targetMeta, DialectMeta.getByName(DialectName.MYSQL));
    }

    @Test
    public void testTransformDate() {
        BaseDataType transform = oracle2MysqlDataTypeTransformer.convert(DataTypeUtil.simpleType(DataTypeEnums.DATE));
        assertEquals(transform.getTypeName(), DataTypeEnums.DATETIME);
    }

    @Test
    public void testTransformBfile() {
        BaseDataType transform = oracle2MysqlDataTypeTransformer.convert(
            new GenericDataType(new Identifier("BFILE")));
        assertEquals(transform.getTypeName(), DataTypeEnums.VARCHAR);
        GenericDataType genericDataType = (GenericDataType)transform;
        List<DataTypeParameter> arguments = genericDataType.getArguments();
        assertEquals(1, arguments.size());
        assertEquals(arguments.get(0), new NumericParameter("255"));
    }

    @Test
    public void testTransformBinaryFloat() {
        assertDataType(
            new GenericDataType(new Identifier("BINARY_FLOAT")), DataTypeUtil.simpleType(DataTypeEnums.FLOAT));
    }

    @Test
    public void testTransformBinaryDouble() {
        assertDataType(new GenericDataType(new Identifier("BINARY_DOUBLE")),
            DataTypeUtil.simpleType(DataTypeEnums.DOUBLE));
    }

    @Test
    public void testBlob() {
        assertDataType(new GenericDataType(new Identifier("BLOB")), new GenericDataType(new Identifier("LONGBLOB")));
    }

    @Test
    public void testChar() {
        assertDataType(DataTypeUtil.simpleType(DataTypeEnums.CHAR, new NumericParameter("10")),
            DataTypeUtil.simpleType(DataTypeEnums.CHAR, new NumericParameter("10")));

        assertDataType(DataTypeUtil.simpleType(DataTypeEnums.CHAR, new NumericParameter("256")),
            DataTypeUtil.simpleType(DataTypeEnums.VARCHAR, new NumericParameter("256")));

        assertDataType(DataTypeUtil.simpleType(DataTypeEnums.CHAR, new NumericParameter("2001")),
            DataTypeUtil.simpleType(DataTypeEnums.TEXT));
    }

    @Test
    public void testNumber() {
        assertDataType(new GenericDataType(new Identifier("NUMBER"), ImmutableList.of(new NumericParameter("1"))),
            DataTypeUtil.simpleType(DataTypeEnums.TINYINT));
        assertDataType(new GenericDataType(new Identifier("NUMBER"), ImmutableList.of(new NumericParameter("3"))),
            DataTypeUtil.simpleType(DataTypeEnums.SMALLINT));
        assertDataType(new GenericDataType(new Identifier("NUMBER"), ImmutableList.of(new NumericParameter("5"))),
            DataTypeUtil.simpleType(DataTypeEnums.INT));
        assertDataType(new GenericDataType(new Identifier("NUMBER"), ImmutableList.of(new NumericParameter("10"))),
            DataTypeUtil.simpleType(DataTypeEnums.BIGINT));
        assertDataType(new GenericDataType(new Identifier("NUMBER"), ImmutableList.of(new NumericParameter("20"))),
            DataTypeUtil.simpleType(DataTypeEnums.DECIMAL, new NumericParameter("20")));

        assertDataType(new GenericDataType(new Identifier("NUMBER"),
                ImmutableList.of(new NumericParameter("20"), new NumericParameter("2"))),
            DataTypeUtil.simpleType(DataTypeEnums.DECIMAL, new NumericParameter("20"), new NumericParameter("2")));

    }

    @Test
    public void testIgnore() {
        assertDataType(
            new GenericDataType(new Identifier("number"), ImmutableList.of(new NumericParameter("1"))),
            DataTypeUtil.simpleType(DataTypeEnums.TINYINT));

    }

    @Test
    public void testDoublePrecious() {
        assertDataType(new GenericDataType(new Identifier("DOUBLE PRECISION")),
            new GenericDataType(new Identifier("DOUBLE PRECISION")));
    }

    @Test
    public void testNvarchar2() {
        assertDataType(new GenericDataType(new Identifier("NVARCHAR2"), ImmutableList.of(new NumericParameter("10"))),
            new GenericDataType(new Identifier("NVARCHAR"), ImmutableList.of(new NumericParameter("10")))
        );
    }

    @Test
    public void testRaw() {
        assertDataType(new GenericDataType(new Identifier("RAW"), ImmutableList.of(new NumericParameter("10"))),
            new GenericDataType(new Identifier("VARBINARY"), ImmutableList.of(new NumericParameter("10")))
        );
    }

    @Test
    public void testSmallInt() {
        assertDataType(new GenericDataType(new Identifier("SMALLINT")),
            new GenericDataType(new Identifier("DECIMAL"), ImmutableList.of(new NumericParameter("38"))));
    }

    @Test
    public void testNchar() {
        assertDataType(new GenericDataType(new Identifier("nchar"), ImmutableList.of(new NumericParameter("256"))),
            new GenericDataType(new Identifier("NVARCHAR"), ImmutableList.of(new NumericParameter("256"))));
    }

    @Test
    public void testTimestamp() {
        assertDataType(new GenericDataType(new Identifier("TIMESTAMP"), ImmutableList.of(new NumericParameter("10"))),
            new GenericDataType(new Identifier("DATETIME"), ImmutableList.of(new NumericParameter("10")))
        );
    }

    @Test
    public void testVarchar() {
        assertDataType(new GenericDataType(new Identifier("VARCHAR"), ImmutableList.of(new NumericParameter("10"))),
            new GenericDataType(new Identifier("VARCHAR"), ImmutableList.of(new NumericParameter("10")))
        );
    }

    @Test
    public void testInterval() {
        assertDataType(new GenericDataType(new Identifier("INTERVAL YEAR")),
            new GenericDataType(new Identifier("VARCHAR"), ImmutableList.of(new NumericParameter("30"))));
    }

    @Test
    public void testUrow() {
        assertDataType(new GenericDataType(new Identifier("UROWID")), new GenericDataType(new Identifier("VARCHAR")));
    }

    private void assertDataType(BaseDataType source, BaseDataType assertDataType) {
        BaseDataType transform = oracle2MysqlDataTypeTransformer.convert(source);
        assertEquals(transform, assertDataType);
    }
}