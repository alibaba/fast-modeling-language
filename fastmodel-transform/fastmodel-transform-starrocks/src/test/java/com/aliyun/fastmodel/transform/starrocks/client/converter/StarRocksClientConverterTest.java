package com.aliyun.fastmodel.transform.starrocks.client.converter;

import java.util.ArrayList;
import java.util.List;

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
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.starrocks.client.property.table.SingleRangePartitionProperty;
import com.aliyun.fastmodel.transform.starrocks.client.property.table.partition.ArrayClientPartitionKey;
import com.aliyun.fastmodel.transform.starrocks.client.property.table.partition.BaseClientPartitionKey;
import com.aliyun.fastmodel.transform.starrocks.client.property.table.partition.LessThanClientPartitionKey;
import com.aliyun.fastmodel.transform.starrocks.client.property.table.partition.MultiRangeClientPartition;
import com.aliyun.fastmodel.transform.starrocks.client.property.table.partition.PartitionClientValue;
import com.aliyun.fastmodel.transform.starrocks.client.property.table.partition.SingleRangeClientPartition;
import com.aliyun.fastmodel.transform.starrocks.format.StarRocksProperty;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.ArrayPartitionKey;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.LessThanPartitionKey;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.ListPartitionValue;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.MultiRangePartition;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.PartitionDesc;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.PartitionValue;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.RangePartitionedBy;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.SingleRangePartition;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Desc:
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
        assertEquals(StarRocksProperty.TABLE_RANGE_PARTITION.getValue(), baseClientProperty.getKey());

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
        assertEquals(StarRocksProperty.TABLE_RANGE_PARTITION.getValue(), baseClientProperty.getKey());
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
        assertEquals(StarRocksProperty.TABLE_RANGE_PARTITION.getValue(), baseClientProperty.getKey());
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
        assertEquals(StarRocksProperty.TABLE_RANGE_PARTITION.getValue(), baseClientProperty.getKey());
        MultiRangeClientPartition value1 = (MultiRangeClientPartition)baseClientProperty.getValue();
        String start = value1.getStart();
        assertEquals("2021-01-01", start);
        assertEquals("2022-01-01", value1.getEnd());
        assertEquals(DateTimeEnum.DAY, value1.getDateTimeEnum());
        assertTrue(value1.getInterval() == Long.parseLong("10"));
    }

    @Test
    public void getDataType() {
        Column column = Column.builder()
            .dataType("int")
            .build();
        starRocksClientConverter.getDataType(column);
    }
}