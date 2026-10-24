/*
 * Copyright 2026 Alibaba Group Holding Ltd.
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

package com.aliyun.fastmodel.transform.mysql.parser.tree;

import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName.Dimension;
import com.aliyun.fastmodel.transform.api.datatype.simple.SimpleDataTypeName;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * MysqlDataTypeName测试类
 *
 * @author panguanjing
 */
public class MysqlDataTypeNameTest {

    @Test
    public void testGetByValueWithSimpleType() {
        assertEquals(MysqlDataTypeName.INT, MysqlDataTypeName.getByValue("int"));
        assertEquals(MysqlDataTypeName.INT, MysqlDataTypeName.getByValue(" INT "));
        assertEquals(MysqlDataTypeName.VARCHAR, MysqlDataTypeName.getByValue("varchar(255)"));
        assertEquals(MysqlDataTypeName.ENUM, MysqlDataTypeName.getByValue("enum<a,b>"));
    }

    @Test
    public void testGetByValueWithUnsigned() {
        assertUnsigned("TINYINT", MysqlDataTypeName.TINYINT, Dimension.ONE);
        assertUnsigned("SMALLINT", MysqlDataTypeName.SMALLINT, Dimension.ONE);
        assertUnsigned("MEDIUMINT", MysqlDataTypeName.MEDIUMINT, Dimension.ONE);
        assertUnsigned("INT", MysqlDataTypeName.INT, Dimension.ONE);
        assertUnsigned("INTEGER", MysqlDataTypeName.INTEGER, Dimension.ONE);
        assertUnsigned("BIGINT", MysqlDataTypeName.BIGINT, Dimension.ONE);
        assertUnsigned("FLOAT", MysqlDataTypeName.FLOAT, Dimension.ONE);
        assertUnsigned("DOUBLE", MysqlDataTypeName.DOUBLE, Dimension.TWO);
        assertUnsigned("REAL", MysqlDataTypeName.REAL, Dimension.TWO);
        assertUnsigned("DECIMAL", MysqlDataTypeName.DECIMAL, Dimension.TWO);
        assertUnsigned("NUMERIC", MysqlDataTypeName.NUMERIC, Dimension.TWO);
    }

    @Test
    public void testGetByValueWithLowerCaseAndBlank() {
        IDataTypeName dataTypeName = MysqlDataTypeName.getByValue("  int(10)   unsigned  ");
        assertEquals("INT UNSIGNED", dataTypeName.getValue());
        assertEquals(Dimension.ONE, dataTypeName.getDimension());

        dataTypeName = MysqlDataTypeName.getByValue("bigint  unsigned  zerofill");
        assertEquals("BIGINT UNSIGNED ZEROFILL", dataTypeName.getValue());
    }

    @Test
    public void testGetByValueWithZeroFill() {
        IDataTypeName dataTypeName = MysqlDataTypeName.getByValue("decimal(10,2) unsigned zerofill");
        assertEquals("DECIMAL UNSIGNED ZEROFILL", dataTypeName.getValue());
        assertEquals(Dimension.TWO, dataTypeName.getDimension());
        assertEquals(SimpleDataTypeName.NUMBER, ((MysqlModifierDataTypeName)dataTypeName).getSimpleDataTypeName());
    }

    @Test
    public void testGetByValueWithSigned() {
        IDataTypeName dataTypeName = MysqlDataTypeName.getByValue("int signed");
        assertEquals("INT SIGNED", dataTypeName.getValue());
    }

    @Test
    public void testGetByValueWithAlias() {
        assertEquals(MysqlDataTypeName.BOOLEAN, MysqlDataTypeName.getByValue("bool"));
        assertEquals(MysqlDataTypeName.CHAR, MysqlDataTypeName.getByValue("character"));
        assertEquals(MysqlDataTypeName.DECIMAL, MysqlDataTypeName.getByValue("dec"));
        assertEquals(MysqlDataTypeName.DECIMAL, MysqlDataTypeName.getByValue("fixed(10,2)"));
        assertEquals(MysqlDataTypeName.DOUBLE, MysqlDataTypeName.getByValue("double precision"));
        assertEquals(MysqlDataTypeName.MEDIUMINT, MysqlDataTypeName.getByValue("middleint"));
        assertEquals(MysqlDataTypeName.TINYINT, MysqlDataTypeName.getByValue("int1"));
        assertEquals(MysqlDataTypeName.SMALLINT, MysqlDataTypeName.getByValue("int2"));
        assertEquals(MysqlDataTypeName.MEDIUMINT, MysqlDataTypeName.getByValue("int3"));
        assertEquals(MysqlDataTypeName.INT, MysqlDataTypeName.getByValue("int4"));
        assertEquals(MysqlDataTypeName.BIGINT, MysqlDataTypeName.getByValue("int8"));
        assertEquals(MysqlDataTypeName.FLOAT, MysqlDataTypeName.getByValue("float4"));
        assertEquals(MysqlDataTypeName.DOUBLE, MysqlDataTypeName.getByValue("float8"));
        assertEquals(MysqlDataTypeName.GEOMETRYCOLLECTION, MysqlDataTypeName.getByValue("geomcollection"));
        assertEquals(MysqlDataTypeName.LONG_VARCHAR, MysqlDataTypeName.getByValue("long"));
        assertEquals(MysqlDataTypeName.CHAR, MysqlDataTypeName.getByValue("national char"));
        assertEquals(MysqlDataTypeName.VARCHAR, MysqlDataTypeName.getByValue("national varchar"));
    }

    @Test
    public void testGetByValueWithMissingType() {
        assertEquals(MysqlDataTypeName.SERIAL, MysqlDataTypeName.getByValue("serial"));
        assertEquals(MysqlDataTypeName.NCHAR, MysqlDataTypeName.getByValue("nchar"));
        assertEquals(MysqlDataTypeName.NVARCHAR, MysqlDataTypeName.getByValue("nvarchar(64)"));
        assertEquals(MysqlDataTypeName.LONG_VARCHAR, MysqlDataTypeName.getByValue("long varchar"));
        assertEquals(MysqlDataTypeName.LONG_VARBINARY, MysqlDataTypeName.getByValue("long varbinary"));
        assertEquals(MysqlDataTypeName.REAL, MysqlDataTypeName.getByValue("real"));
        assertEquals(MysqlDataTypeName.BOOLEAN, MysqlDataTypeName.getByValue("boolean"));
    }

    /**
     * MySQL 8.0 全部数据类型及常用别名，确保类型枚举不再有缺失
     */
    @Test
    public void testGetByValueWithAllMysqlDataType() {
        String[] dataTypes = {
            "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT", "MIDDLEINT",
            "INT1", "INT2", "INT3", "INT4", "INT8", "REAL", "DOUBLE", "DOUBLE PRECISION",
            "DECIMAL", "DEC", "FIXED", "NUMERIC", "FLOAT", "FLOAT4", "FLOAT8", "BIT",
            "BOOL", "BOOLEAN", "SERIAL", "DATE", "DATETIME", "TIMESTAMP", "TIME", "YEAR",
            "CHAR", "CHARACTER", "VARCHAR", "NCHAR", "NVARCHAR", "NATIONAL CHAR",
            "NATIONAL CHARACTER", "NATIONAL VARCHAR", "LONG", "LONG VARCHAR", "LONG VARBINARY",
            "BINARY", "VARBINARY", "TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB",
            "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT", "ENUM", "SET", "JSON",
            "GEOMETRY", "POINT", "LINESTRING", "POLYGON", "MULTIPOINT", "MULTILINESTRING",
            "MULTIPOLYGON", "GEOMETRYCOLLECTION", "GEOMCOLLECTION"
        };
        for (String dataType : dataTypes) {
            assertNotNull(dataType, MysqlDataTypeName.getByValue(dataType));
        }
        String[] unsignedDataTypes = {
            "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT", "REAL", "DOUBLE",
            "DOUBLE PRECISION", "DECIMAL", "DEC", "FIXED", "NUMERIC", "FLOAT", "FLOAT4", "FLOAT8"
        };
        for (String dataType : unsignedDataTypes) {
            String baseValue = MysqlDataTypeName.getBaseDataTypeName(dataType).getValue();
            assertEquals(baseValue + " UNSIGNED", MysqlDataTypeName.getByValue(dataType + " UNSIGNED").getValue());
        }
    }

    @Test
    public void testGetByValueWithUnsupportedType() {
        try {
            MysqlDataTypeName.getByValue("unknown_type");
            fail("expect IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("unknown_type"));
        }
    }

    @Test
    public void testGetBaseDataTypeName() {
        assertEquals(MysqlDataTypeName.INT, MysqlDataTypeName.getBaseDataTypeName("int unsigned"));
        assertEquals(MysqlDataTypeName.DECIMAL, MysqlDataTypeName.getBaseDataTypeName("decimal(10,2) unsigned"));
        assertEquals(MysqlDataTypeName.BIGINT, MysqlDataTypeName.getBaseDataTypeName("bigint unsigned zerofill"));
    }

    @Test
    public void testGetBaseValue() {
        assertEquals("INT", MysqlDataTypeName.getBaseValue("int(10) unsigned"));
        assertEquals("DECIMAL", MysqlDataTypeName.getBaseValue("decimal(10,2) unsigned"));
    }

    @Test
    public void testGetModifiers() {
        assertEquals("", MysqlDataTypeName.getModifiers("int"));
        assertEquals("UNSIGNED", MysqlDataTypeName.getModifiers("int unsigned"));
        assertEquals("UNSIGNED ZEROFILL", MysqlDataTypeName.getModifiers("int(10) unsigned zerofill"));
        assertEquals("SIGNED", MysqlDataTypeName.getModifiers("int signed"));
        assertEquals("ZEROFILL", MysqlDataTypeName.getModifiers("int zerofill"));
    }

    @Test
    public void testHasModifier() {
        assertTrue(MysqlDataTypeName.hasModifier("int unsigned"));
        assertTrue(MysqlDataTypeName.hasModifier("bigint unsigned zerofill"));
        assertFalse(MysqlDataTypeName.hasModifier("int"));
        assertFalse(MysqlDataTypeName.hasModifier("varchar(10)"));
    }

    private void assertUnsigned(String base, MysqlDataTypeName expect, Dimension dimension) {
        IDataTypeName dataTypeName = MysqlDataTypeName.getByValue(base + " unsigned");
        assertEquals(expect.getValue() + " UNSIGNED", dataTypeName.getValue());
        assertEquals(dimension, dataTypeName.getDimension());
        assertEquals(expect.getName(), dataTypeName.getName());
        assertEquals(expect.getSimpleDataTypeName(), ((MysqlModifierDataTypeName)dataTypeName).getSimpleDataTypeName());
        assertEquals(expect, MysqlDataTypeName.getBaseDataTypeName(base + " unsigned"));
    }
}
