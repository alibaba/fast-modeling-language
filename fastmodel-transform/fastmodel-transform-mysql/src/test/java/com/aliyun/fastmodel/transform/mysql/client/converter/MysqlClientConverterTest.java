package com.aliyun.fastmodel.transform.mysql.client.converter;

import com.aliyun.fastmodel.core.parser.LanguageParser;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.transform.api.client.PropertyConverter;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * MysqlClientConverter测试类
 *
 * @author panguanjing
 * @date 2025/10/11
 */
public class MysqlClientConverterTest {

    MysqlClientConverter mysqlClientConverter = new MysqlClientConverter();

    @Test
    public void testGetLanguageParser() {
        LanguageParser languageParser = mysqlClientConverter.getLanguageParser();
        assertNotNull(languageParser);
    }

    @Test
    public void testGetPropertyConverter() {
        PropertyConverter propertyConverter = mysqlClientConverter.getPropertyConverter();
        assertNotNull(propertyConverter);
    }

    @Test
    public void testGetDataTypeWithSimpleType() {
        Column column = Column.builder().name("c1").dataType("bigint").build();
        BaseDataType dataType = mysqlClientConverter.getDataType(column);
        assertNotNull(dataType);
        assertEquals("BIGINT", dataType.toString());
    }

    @Test
    public void testGetDataTypeWithParameterizedType() {
        Column column = Column.builder().name("c1").dataType("varchar(255)").build();
        BaseDataType dataType = mysqlClientConverter.getDataType(column);
        assertNotNull(dataType);
        assertEquals("VARCHAR(255)", dataType.toString());
    }

    @Test
    public void testGetDataTypeWithTwoParameterType() {
        Column column = Column.builder().name("c1").dataType("decimal(10,2)").build();
        BaseDataType dataType = mysqlClientConverter.getDataType(column);
        assertNotNull(dataType);
        assertEquals("DECIMAL(10,2)", dataType.toString());
    }

    @Test
    public void testGetDataTypeWithUnknownType() {
        Column column = Column.builder().name("c1").dataType("unknown_type").build();
        BaseDataType dataType = mysqlClientConverter.getDataType(column);
        assertNull(dataType);
    }

    @Test
    public void testCreateProperty() {
        MysqlPropertyConverter mysqlPropertyConverter = new MysqlPropertyConverter();
        // 测试基础属性创建
        assertNotNull(mysqlPropertyConverter);
    }
}