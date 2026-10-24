package com.aliyun.fastmodel.transform.mysql.client.converter;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.aliyun.fastmodel.core.parser.LanguageParser;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.transform.api.client.PropertyConverter;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.client.dto.table.TableConfig;
import com.aliyun.fastmodel.transform.mysql.format.MysqlVisitor;
import com.aliyun.fastmodel.transform.mysql.context.MysqlTransformContext;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
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
    public void testGetDataTypeWithUnsignedType() {
        Column column = Column.builder().name("c1").dataType("INT UNSIGNED").length(10).build();
        BaseDataType dataType = mysqlClientConverter.getDataType(column);
        assertNotNull(dataType);
        assertEquals("INT(10) UNSIGNED", dataType.toString());
    }

    @Test
    public void testGetDataTypeWithUnsignedDecimal() {
        Column column = Column.builder().name("c1").dataType("DECIMAL UNSIGNED").precision(10).scale(2).build();
        BaseDataType dataType = mysqlClientConverter.getDataType(column);
        assertNotNull(dataType);
        assertEquals("DECIMAL(10,2) UNSIGNED", dataType.toString());
    }

    @Test
    public void testGetDataTypeWithUnsignedZeroFill() {
        Column column = Column.builder().name("c1").dataType("BIGINT UNSIGNED ZEROFILL").length(20).build();
        BaseDataType dataType = mysqlClientConverter.getDataType(column);
        assertNotNull(dataType);
        assertEquals("BIGINT(20) UNSIGNED ZEROFILL", dataType.toString());
    }

    @Test
    public void testCreateProperty() {
        MysqlPropertyConverter mysqlPropertyConverter = new MysqlPropertyConverter();
        // 测试基础属性创建
        assertNotNull(mysqlPropertyConverter);
    }

    @Test
    public void testParameterizedModifierTypeRetainsInlineParameters() {
        Column integer = Column.builder().name("c1").dataType("INT(10) UNSIGNED").build();
        assertEquals("INT(10) UNSIGNED", mysqlClientConverter.getDataType(integer).toString());
        Column decimal = Column.builder().name("c2").dataType("DECIMAL(12,3) UNSIGNED").build();
        assertEquals("DECIMAL(12,3) UNSIGNED", mysqlClientConverter.getDataType(decimal).toString());
    }

    @Test
    public void testModifiersSurviveClientRoundTrip() {
        Node node = (Node)mysqlClientConverter.getLanguageParser().parseNode(
            "CREATE TABLE sample_types (a INT(10) UNSIGNED, b BIGINT ZEROFILL, c DECIMAL(12,3) SIGNED)");
        Table table = mysqlClientConverter.convertToTable(node, MysqlTransformContext.builder().build());
        assertEquals("INT UNSIGNED", table.getColumns().get(0).getDataType());
        assertEquals("BIGINT ZEROFILL", table.getColumns().get(1).getDataType());
        assertEquals("DECIMAL SIGNED", table.getColumns().get(2).getDataType());
        assertEquals("INT(10) UNSIGNED", mysqlClientConverter.getDataType(table.getColumns().get(0)).toString());
        assertEquals("BIGINT ZEROFILL", mysqlClientConverter.getDataType(table.getColumns().get(1)).toString());
        assertEquals("DECIMAL(12,3) SIGNED", mysqlClientConverter.getDataType(table.getColumns().get(2)).toString());
    }

    @Test
    public void testModifiedIntegerPrimaryKeyRetainsAutoIncrement() {
        for (String modifier : new String[] {"UNSIGNED", "SIGNED", "UNSIGNED ZEROFILL"}) {
            String ddl = roundTripSql("CREATE TABLE sample_types (id INT(10) " + modifier + " NOT NULL PRIMARY KEY)", true);
            assertTrue(ddl, ddl.contains("INT(10) " + modifier));
            assertTrue(ddl, ddl.contains("AUTO_INCREMENT"));
            assertTrue(ddl, ddl.contains("PRIMARY KEY"));
        }
    }

    @Test
    public void testModifiedNumericNullDefaultRemainsSqlNull() {
        Table table = Table.builder().name("sample_types").columns(Arrays.asList(
            Column.builder().name("a").dataType("INT UNSIGNED").nullable(true).defaultValue("NULL").build(),
            Column.builder().name("b").dataType("DECIMAL(12,3) SIGNED").nullable(true).defaultValue("NULL").build(),
            Column.builder().name("c").dataType("BIGINT ZEROFILL").nullable(true).defaultValue("NULL").build()
        )).build();
        Node node = mysqlClientConverter.convertToNode(table, TableConfig.builder().build());
        MysqlVisitor visitor = new MysqlVisitor(MysqlTransformContext.builder().autoIncrement(false).build());
        node.accept(visitor, 0);
        String ddl = visitor.getBuilder().toString();
        assertTrue(ddl, ddl.contains("INT UNSIGNED NULL DEFAULT NULL"));
        assertTrue(ddl, ddl.contains("DECIMAL(12,3) SIGNED NULL DEFAULT NULL"));
        assertTrue(ddl, ddl.contains("BIGINT ZEROFILL NULL DEFAULT NULL"));
        assertFalse(ddl, ddl.contains("DEFAULT 'NULL'"));
    }

    @Test
    public void testEquivalentModifiedTypesHaveStableHashCode() {
        BaseDataType first = mysqlClientConverter.getDataType(
            Column.builder().name("a").dataType("int(10) unsigned").build());
        BaseDataType second = mysqlClientConverter.getDataType(
            Column.builder().name("b").dataType("INT(10) UNSIGNED").build());
        assertEquals(first, second);
        assertEquals(first.hashCode(), second.hashCode());
        Set<BaseDataType> types = new HashSet<>();
        types.add(first);
        assertTrue(types.contains(second));
        types.add(second);
        assertEquals(1, types.size());
    }

    private String roundTripSql(String ddl, boolean autoIncrement) {
        Node parsed = (Node)mysqlClientConverter.getLanguageParser().parseNode(ddl);
        Table table = mysqlClientConverter.convertToTable(parsed, MysqlTransformContext.builder().build());
        Node restored = mysqlClientConverter.convertToNode(table, TableConfig.builder().build());
        MysqlVisitor visitor = new MysqlVisitor(MysqlTransformContext.builder().autoIncrement(autoIncrement).build());
        restored.accept(visitor, 0);
        return visitor.getBuilder().toString();
    }
}
