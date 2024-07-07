package com.aliyun.fastmodel.transform.starrocks.client.converter;

import java.util.ArrayList;
import java.util.List;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.enums.DateTimeEnum;
import com.aliyun.fastmodel.core.tree.expr.literal.IntervalLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.ListStringLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.transform.api.client.PropertyConverter;
import com.aliyun.fastmodel.transform.api.client.dto.constraint.Constraint;
import com.aliyun.fastmodel.transform.api.client.dto.index.Index;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.client.dto.table.TableConfig;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.api.extension.client.property.column.AggrColumnProperty;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.SingleRangePartitionProperty;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.ArrayClientPartitionKey;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.BaseClientPartitionKey;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.LessThanClientPartitionKey;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.MultiRangeClientPartition;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.PartitionClientValue;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.SingleRangeClientPartition;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.RangePartitionedBy;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.desc.MultiRangePartition;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.desc.PartitionDesc;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.desc.SingleRangePartition;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.keyvalue.ArrayPartitionKey;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.keyvalue.LessThanPartitionKey;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.keyvalue.ListPartitionValue;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.keyvalue.PartitionValue;
import com.aliyun.fastmodel.transform.starrocks.StarRocksTransformer;
import com.aliyun.fastmodel.transform.starrocks.context.StarRocksContext;
import com.aliyun.fastmodel.transform.starrocks.parser.StarRocksLanguageParser;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import static com.aliyun.fastmodel.transform.api.extension.client.property.ExtensionPropertyKey.TABLE_RANGE_PARTITION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * StarRocksClientConverterTest
 *
 * @author panguanjing
 * @date 2023/9/16
 */
public class StarRocksClientConverterTest {

    StarRocksClientConverter starRocksClientConverter = new StarRocksClientConverter();

    @Test
    public void getPropertyConverter() {
        PropertyConverter propertyConverter = starRocksClientConverter.getPropertyConverter();
        assertNotNull(propertyConverter);
    }

    @Test
    public void testToBaseClientProperty() {
        List<ColumnDefinition> columns = Lists.newArrayList();
        columns.add(ColumnDefinition.builder().colName(new Identifier("k1")).build());
        List<PartitionDesc> rangePartitions = Lists.newArrayList();
        List<ListPartitionValue> partitionValues = Lists.newArrayList();
        ArrayList<PartitionValue> value = Lists.newArrayList(new PartitionValue(new StringLiteral("2020-01-01")),
            new PartitionValue(new StringLiteral("2022-01-01")));
        partitionValues.add(new ListPartitionValue(value));
        List<Property> properties = Lists.newArrayList(new Property("a", "b"));
        SingleRangePartition singleRangePartition = new SingleRangePartition(
            new Identifier("abc"),
            false,
            new ArrayPartitionKey(partitionValues),
            properties
        );
        rangePartitions.add(singleRangePartition);
        CreateTable table = CreateTable.builder()
            .tableName(QualifiedName.of("abc"))
            .partition(new RangePartitionedBy(columns, rangePartitions))
            .build();
        List<BaseClientProperty> baseClientProperties = starRocksClientConverter.toBaseClientProperty(table);
        assertEquals(1, baseClientProperties.size());
        BaseClientProperty baseClientProperty = baseClientProperties.get(0);
        assertEquals(TABLE_RANGE_PARTITION.getValue(), baseClientProperty.getKey());

        SingleRangePartitionProperty singleRangePartitionProperty = (SingleRangePartitionProperty)baseClientProperty;
        SingleRangeClientPartition value1 = singleRangePartitionProperty.getValue();
        assertEquals("abc", value1.getName());
        BaseClientPartitionKey partitionKey = value1.getPartitionKey();
        ArrayClientPartitionKey arrayClientPartitionKey = (ArrayClientPartitionKey)partitionKey;
        List<List<PartitionClientValue>> partitionValue = arrayClientPartitionKey.getPartitionValue();
        assertEquals(1, partitionValue.size());
        List<PartitionClientValue> strings = partitionValue.get(0);
        assertEquals(2, strings.size());
    }

    @Test
    public void testToBaseClientPropertyLessThan() {
        List<ColumnDefinition> columns = Lists.newArrayList();
        columns.add(ColumnDefinition.builder().colName(new Identifier("k1")).build());
        List<PartitionDesc> rangeParations = Lists.newArrayList();
        ArrayList<PartitionValue> value = Lists.newArrayList(new PartitionValue(new StringLiteral("2020-01-01")),
            new PartitionValue(new StringLiteral("2022-01-01")));
        ListPartitionValue e = new ListPartitionValue(value);
        SingleRangePartition singleRangePartition = new SingleRangePartition(
            new Identifier("abc"),
            false,
            new LessThanPartitionKey(new ListPartitionValue(value)),
            null
        );
        rangeParations.add(singleRangePartition);
        CreateTable table = CreateTable.builder()
            .tableName(QualifiedName.of("abc"))
            .partition(new RangePartitionedBy(columns, rangeParations))
            .build();
        List<BaseClientProperty> baseClientProperties = starRocksClientConverter.toBaseClientProperty(table);
        assertEquals(1, baseClientProperties.size());
        BaseClientProperty baseClientProperty = baseClientProperties.get(0);
        assertEquals(TABLE_RANGE_PARTITION.getValue(), baseClientProperty.getKey());
        SingleRangeClientPartition value1 = (SingleRangeClientPartition)baseClientProperty.getValue();
        assertEquals("abc", value1.getName());
        assertFalse(value1.isIfNotExists());
        BaseClientPartitionKey partitionKey = value1.getPartitionKey();
        LessThanClientPartitionKey lessThanClientPartitionKey = (LessThanClientPartitionKey)partitionKey;
        assertEquals(2, lessThanClientPartitionKey.getPartitionValueList().size());
        assertFalse(lessThanClientPartitionKey.isMaxValue());
    }

    @Test
    public void testToBaseClientPropertyMultiRange() {
        List<ColumnDefinition> columns = Lists.newArrayList();
        columns.add(ColumnDefinition.builder().colName(new Identifier("k1")).build());
        List<PartitionDesc> rangePartitions = Lists.newArrayList();
        ArrayList<StringLiteral> value = Lists.newArrayList(new StringLiteral("2020-01-01"), new StringLiteral("2022-01-01"));
        ListStringLiteral e = new ListStringLiteral(value);
        MultiRangePartition singleRangePartition = new MultiRangePartition(
            new StringLiteral("2021-01-01"),
            new StringLiteral("2022-01-01"),
            null,
            new LongLiteral("1")
        );
        rangePartitions.add(singleRangePartition);
        CreateTable table = CreateTable.builder()
            .tableName(QualifiedName.of("abc"))
            .partition(new RangePartitionedBy(columns, rangePartitions))
            .build();
        List<BaseClientProperty> baseClientProperties = starRocksClientConverter.toBaseClientProperty(table);
        assertEquals(1, baseClientProperties.size());
        BaseClientProperty baseClientProperty = baseClientProperties.get(0);
        assertEquals(TABLE_RANGE_PARTITION.getValue(), baseClientProperty.getKey());
        MultiRangeClientPartition value1 = (MultiRangeClientPartition)baseClientProperty.getValue();
        String start = value1.getStart();
        assertEquals("2021-01-01", start);
        assertEquals("2022-01-01", value1.getEnd());
        assertEquals(null, value1.getDateTimeEnum());
        assertTrue(value1.getInterval().intValue() == 1);
    }

    @Test
    public void testToBaseClientPropertyMultiRangeInterval() {
        List<ColumnDefinition> columns = Lists.newArrayList();
        columns.add(ColumnDefinition.builder().colName(new Identifier("k1")).build());
        List<PartitionDesc> rangePartitions = Lists.newArrayList();
        ArrayList<StringLiteral> value = Lists.newArrayList(new StringLiteral("2020-01-01"), new StringLiteral("2022-01-01"));
        ListStringLiteral e = new ListStringLiteral(value);
        MultiRangePartition singleRangePartition = new MultiRangePartition(
            new StringLiteral("2021-01-01"),
            new StringLiteral("2022-01-01"),
            new IntervalLiteral(new LongLiteral("10"), DateTimeEnum.DAY),
            null
        );
        rangePartitions.add(singleRangePartition);
        CreateTable table = CreateTable.builder()
            .tableName(QualifiedName.of("abc"))
            .partition(new RangePartitionedBy(columns, rangePartitions))
            .build();
        List<BaseClientProperty> baseClientProperties = starRocksClientConverter.toBaseClientProperty(table);
        assertEquals(1, baseClientProperties.size());
        BaseClientProperty baseClientProperty = baseClientProperties.get(0);
        assertEquals(TABLE_RANGE_PARTITION.getValue(), baseClientProperty.getKey());
        MultiRangeClientPartition value1 = (MultiRangeClientPartition)baseClientProperty.getValue();
        String start = value1.getStart();
        assertEquals("2021-01-01", start);
        assertEquals("2022-01-01", value1.getEnd());
        assertEquals(DateTimeEnum.DAY, value1.getDateTimeEnum());
        assertTrue(value1.getInterval() == Long.parseLong("10"));
    }

    @Test
    public void testConvertIndexToTable() {
        StarRocksLanguageParser starRocksLanguageParser = new StarRocksLanguageParser();
        Node node1 = starRocksLanguageParser.parseNode("CREATE TABLE example_db.table_hash\n"
            + "(\n"
            + "    k1 TINYINT,\n"
            + "    k2 DECIMAL(10, 2) DEFAULT \"10.5\",\n"
            + "    v1 CHAR(10) REPLACE,\n"
            + "    v2 INT SUM,\n"
            + "    INDEX k1_idx (k1) USING BITMAP COMMENT 'xxxxxx'\n"
            + ")\n"
            + "ENGINE=olap\n"
            + "AGGREGATE KEY(k1, k2)\n"
            + "COMMENT \"my first starrocks table\"\n"
            + "DISTRIBUTED BY HASH(k1) BUCKETS 10\n"
            + "PROPERTIES (\"storage_type\"=\"column\");");
        Table table = starRocksClientConverter.convertToTable(node1, StarRocksContext.builder().build());
        List<Constraint> constraints = table.getConstraints();
        assertEquals(2, constraints.size());
        List<Index> indices = table.getIndices();
        assertEquals("k1_idx", indices.get(0).getName());
    }

    @Test
    public void testConvertColumnPropertyToProperty() {
        CreateTable createTable = new StarRocksLanguageParser().parseNode(
            "CREATE TABLE IF NOT EXISTS example_db.aggregate_tbl (\n"
                + "    site_id LARGEINT NOT NULL COMMENT \"id of site\",\n"
                + "    date DATE NOT NULL COMMENT \"time of event\",\n"
                + "    city_code VARCHAR(20) COMMENT \"city_code of user\",\n"
                + "    pv BIGINT SUM DEFAULT \"0\" COMMENT \"total page views\"\n"
                + ")\n"
                + "AGGREGATE KEY(site_id, date, city_code)\n"
                + "DISTRIBUTED BY HASH(site_id)\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"3\"\n"
                + ");\n"
                + "\n"
        );
        List<Column> tableColumns = starRocksClientConverter.toTableColumns(createTable);
        Column pv = tableColumns.stream().filter(c -> {
            return StringUtils.equalsIgnoreCase(c.getName(), "pv");
        }).findFirst().get();
        assertNotNull(pv);
        List<BaseClientProperty> properties = pv.getProperties();
        assertEquals(1, properties.size());
        assertEquals("SUM", properties.get(0).getValue().toString());
    }

    @Test
    public void testConvertColumnPropertyToProperty2() {
        String template = "CREATE TABLE IF NOT EXISTS example_db.aggregate_tbl (\n"
            + "    site_id LARGEINT NOT NULL COMMENT \"id of site\",\n"
            + "    date DATE NOT NULL COMMENT \"time of event\",\n"
            + "    city_code VARCHAR(20) COMMENT \"city_code of user\",\n"
            + "    pv BIGINT %s DEFAULT \"0\" COMMENT \"total page views\"\n"
            + ")\n"
            + "AGGREGATE KEY(site_id, date, city_code)\n"
            + "DISTRIBUTED BY HASH(site_id)\n"
            + "PROPERTIES (\n"
            + "\"replication_num\" = \"3\"\n"
            + ");\n"
            + "\n";
        List<String> aggrType = Lists.newArrayList("SUM", "MAX", "MIN", "REPLACE", "HLL_UNION", "BITMAP_UNION");
        for (String a : aggrType) {
            CreateTable createTable = new StarRocksLanguageParser().parseNode(
                String.format(template, a)
            );
            List<Column> tableColumns = starRocksClientConverter.toTableColumns(createTable);
            Column pv = tableColumns.stream().filter(c -> {
                return StringUtils.equalsIgnoreCase(c.getName(), "pv");
            }).findFirst().get();
            assertNotNull(pv);
            List<BaseClientProperty> properties = pv.getProperties();
            assertEquals(1, properties.size());
            assertNotNull(properties.get(0).getValue());
        }
    }

    @Test
    public void testToFmlTable() {
        List<Column> columns = Lists.newArrayList();
        List<BaseClientProperty> properties = Lists.newArrayList();
        AggrColumnProperty e = new AggrColumnProperty();
        e.setValueString("REPLACE");
        properties.add(e);
        columns.add(Column.builder().name("c1").dataType("bigint").properties(properties).build());
        Table table = Table.builder().name("t1").columns(columns).build();
        BaseStatement node = (BaseStatement)starRocksClientConverter.convertToNode(table,
            TableConfig.builder().dialectMeta(DialectMeta.DEFAULT_STARROCKS).build());
        StarRocksTransformer starRocksTransformer = new StarRocksTransformer();
        DialectNode transform = starRocksTransformer.transform(node);
        assertEquals("CREATE TABLE IF NOT EXISTS t1\n"
            + "(\n"
            + "   c1 BIGINT REPLACE NOT NULL\n"
            + ");", transform.getNode());
    }

    @Test
    public void testMapTypeToFmlTable() {
        List<Column> columns = Lists.newArrayList();
        List<BaseClientProperty> properties = Lists.newArrayList();
        AggrColumnProperty e = new AggrColumnProperty();
        e.setValueString("REPLACE");
        properties.add(e);
        columns.add(Column.builder().name("c1").dataType("MAP<STRING, STRING>").properties(properties).build());
        Table table = Table.builder().name("t1").columns(columns).build();
        BaseStatement node = (BaseStatement)starRocksClientConverter.convertToNode(table,
            TableConfig.builder().dialectMeta(DialectMeta.DEFAULT_STARROCKS).build());
        StarRocksTransformer starRocksTransformer = new StarRocksTransformer();
        DialectNode transform = starRocksTransformer.transform(node);
        assertEquals("CREATE TABLE IF NOT EXISTS t1\n"
            + "(\n"
            + "   c1 MAP<STRING,STRING> REPLACE NOT NULL\n"
            + ");", transform.getNode());

    }
}