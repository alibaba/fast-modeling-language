package com.aliyun.fastmodel.transform.starrocks.format;

import java.util.List;

import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeParameter;
import com.aliyun.fastmodel.core.tree.datatype.TypeParameter;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.enums.DateTimeEnum;
import com.aliyun.fastmodel.core.tree.expr.literal.IntervalLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.ListStringLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.table.AddCols;
import com.aliyun.fastmodel.core.tree.statement.table.ChangeCol;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.DropCol;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.RenameTable;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableProperties;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.core.tree.util.IdentifierUtil;
import com.aliyun.fastmodel.transform.starrocks.context.StarRocksContext;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.AggDesc;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.AggregateConstraint;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.DuplicateConstraint;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.datatype.StarRocksGenericDataType;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.LessThanPartitionKey;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.ListPartitionValue;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.ListPartitionedBy;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.MultiItemListPartition;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.MultiRangePartition;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.PartitionDesc;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.PartitionValue;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.RangePartitionedBy;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.SingleItemListPartition;
import com.aliyun.fastmodel.transform.starrocks.parser.tree.partition.SingleRangePartition;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * StarRocksVisitorTest
 *
 * @author panguanjing
 * @date 2023/9/12
 */
public class StarRocksOutVisitorTest {

    StarRocksContext context = StarRocksContext.builder().build();
    StarRocksOutVisitor starRocksVisitor = new StarRocksOutVisitor(context);

    @Test
    public void testVisitCreateTable() {
        StarRocksContext context = StarRocksContext.builder().build();
        StarRocksOutVisitor starRocksVisitor = new StarRocksOutVisitor(context);
        List<ColumnDefinition> columns = Lists.newArrayList();
        List<Property> columnProperties = toColumnProperties();
        ColumnDefinition columnDefinition = ColumnDefinition.builder()
            .colName(new Identifier("c1"))
            .dataType(new StarRocksGenericDataType("int"))
            .defaultValue(new StringLiteral("2001-01-01"))
            .properties(columnProperties)
            .build();
        columns.add(columnDefinition);
        columns.add(ColumnDefinition.builder().colName(new Identifier("c2")).dataType(new StarRocksGenericDataType("int")).build());
        columns.add(ColumnDefinition.builder().colName(new Identifier("c3")).dataType(new StarRocksGenericDataType("int")).build());
        List<BaseConstraint> constraints = toConstraint();
        PartitionedBy partition = toPartition();
        List<Property> properties = toProperty();
        CreateTable createTable = CreateTable.builder()
            .tableName(QualifiedName.of("abc"))
            .partition(partition)
            .columns(columns)
            .constraints(constraints)
            .properties(properties)
            .build();
        starRocksVisitor.visitCreateTable(createTable, 0);
        String s = starRocksVisitor.getBuilder().toString();
        assertEquals("CREATE TABLE abc\n"
            + "(\n"
            + "   c1 INT SUM DEFAULT \"2001-01-01\",\n"
            + "   c2 INT,\n"
            + "   c3 INT\n"
            + ")\n"
            + "ENGINE=mysql\n"
            + "PRIMARY KEY (c1)\n"
            + "DUPLICATE KEY (c2)\n"
            + "AGGREGATE KEY (c2)\n"
            + "PARTITION BY RANGE (c1,c2)\n"
            + "(\n"
            + "   PARTITION p1 VALUES LESS THAN (\"2010-01-10\"),\n"
            + "   START(\"2001-01-01\") END(\"2020-01-01\") EVERY (INTERVAL 10 HOUR)\n"
            + ")", s);
    }

    @Test
    public void testVisitSetTableProperties() {
        List<Property> propertiesList = Lists.newArrayList();
        propertiesList.add(new Property("replication_num", "3"));
        SetTableProperties setTableProperties = new SetTableProperties(
            QualifiedName.of("abc.bcd"),
            propertiesList
        );
        StarRocksContext context = StarRocksContext.builder().build();
        StarRocksOutVisitor starRocksVisitor = new StarRocksOutVisitor(context);
        starRocksVisitor.visitSetTableProperties(setTableProperties, 0);
        String s = starRocksVisitor.getBuilder().toString();
        assertEquals("ALTER TABLE abc.bcd SET (\"replication_num\"=\"3\")", s);
    }

    @Test
    public void testRenameTable() {
        RenameTable renameTable = new RenameTable(QualifiedName.of("a"), QualifiedName.of("b"));
        StarRocksContext context = StarRocksContext.builder().build();
        StarRocksOutVisitor starRocksVisitor = new StarRocksOutVisitor(context);
        starRocksVisitor.visitRenameTable(renameTable, 0);
        String s = starRocksVisitor.getBuilder().toString();
        assertEquals("ALTER TABLE a RENAME b", s);
    }

    @Test
    public void testAddCols() {
        List<ColumnDefinition> list = Lists.newArrayList();
        ColumnDefinition columnDefinition = ColumnDefinition.builder()
            .colName(new Identifier("c1"))
            .dataType(new StarRocksGenericDataType("int"))
            .build();
        list.add(columnDefinition);

        List<DataTypeParameter> arguments = Lists.newArrayList();
        arguments.add(new TypeParameter(DataTypeUtil.simpleType("STRING", null)));
        ColumnDefinition columnDefinition2 = ColumnDefinition.builder()
            .colName(new Identifier("c2"))
            .dataType(new StarRocksGenericDataType("array", arguments))
            .build();
        list.add(columnDefinition2);
        AddCols addCols = new AddCols(QualifiedName.of("a"), list);
        starRocksVisitor.visitAddCols(addCols, 0);
        assertEquals("ALTER TABLE a ADD COLUMN\n"
            + "(\n"
            + "   c1 INT,\n"
            + "   c2 ARRAY<STRING>\n"
            + ")", starRocksVisitor.getBuilder().toString());
    }

    @Test
    public void testDistributeBy() {
        List<ColumnDefinition> columns = Lists.newArrayList();
        List<Property> columnProperties = Lists.newArrayList();
        columns.add(ColumnDefinition.builder().colName(new Identifier("c1")).properties(columnProperties).build());
        List<Property> properties = Lists.newArrayList();
        properties.add(new Property(StarRocksProperty.TABLE_DISTRIBUTED_HASH.getValue(), "c1"));
        properties.add(new Property(StarRocksProperty.TABLE_DISTRIBUTED_BUCKETS.getValue(), "4"));
        CreateTable createTable = CreateTable.builder()
            .tableName(QualifiedName.of("ab"))
            .columns(columns)
            .properties(properties)
            .build();
        starRocksVisitor.visitCreateTable(createTable, 0);
        assertEquals("CREATE TABLE ab\n"
            + "(\n"
            + "   c1\n"
            + ")\n"
            + "DISTRIBUTED BY HASH(c1) BUCKETS 4", starRocksVisitor.getBuilder().toString());
    }

    @Test
    public void testPartitionValueRaw() {
        List<Property> properties = Lists.newArrayList();
        properties.add(new Property(StarRocksProperty.TABLE_PARTITION_RAW.getValue(), "PARTITION BY RANGE (pay_dt) (\n"
            + "  PARTITION p1 VALUES LESS THAN (\"20210102\"),\n"
            + "  PARTITION p2 VALUES LESS THAN (\"20210103\"),\n"
            + "  PARTITION p3 VALUES LESS THAN MAXVALUE\n"
            + ")"));
        List<ColumnDefinition> columns = Lists.newArrayList();
        ColumnDefinition e = ColumnDefinition.builder()
            .colName(new Identifier("c1"))
            .dataType(DataTypeUtil.simpleType("BIGINT", null))
            .build();
        columns.add(e);
        CreateTable createTable = CreateTable.builder()
            .tableName(QualifiedName.of("abc"))
            .columns(columns)
            .properties(properties)
            .build();
        starRocksVisitor.visitCreateTable(createTable, 0);
        String s = starRocksVisitor.getBuilder().toString();
        assertEquals("CREATE TABLE abc\n"
            + "(\n"
            + "   c1 BIGINT\n"
            + ")\n"
            + "PARTITION BY RANGE (pay_dt) (\n"
            + "  PARTITION p1 VALUES LESS THAN (\"20210102\"),\n"
            + "  PARTITION p2 VALUES LESS THAN (\"20210103\"),\n"
            + "  PARTITION p3 VALUES LESS THAN MAXVALUE\n"
            + ")", s);
    }

    @Test
    public void testVisitDropColumn() {
        DropCol dropCol = new DropCol(QualifiedName.of("abc.bcd"), new Identifier("c1"));
        starRocksVisitor.visitDropCol(dropCol, 0);
        String s = starRocksVisitor.getBuilder().toString();
        assertEquals("ALTER TABLE abc.bcd DROP COLUMN c1", s);
    }

    @Test
    public void testVisitChangeColumn() {
        ChangeCol changeCol = new ChangeCol(QualifiedName.of("abc.bcd"), new Identifier("c1"),
            ColumnDefinition.builder().defaultValue(new StringLiteral("1")).colName(new Identifier("c1"))
                .notNull(false)
                .dataType(new StarRocksGenericDataType("string")).build());
        starRocksVisitor.visitChangeCol(changeCol, 0);
        String s = starRocksVisitor.getBuilder().toString();
        assertEquals("ALTER TABLE abc.bcd MODIFY COLUMN c1 STRING NULL DEFAULT \"1\"", s);
    }

    @Test
    public void testVisitListPartition() {
        List<ColumnDefinition> columns = Lists.newArrayList(
            ColumnDefinition.builder().colName(new Identifier("k1")).build()
        );
        ListStringLiteral listStringLiteral = new ListStringLiteral(Lists.newArrayList(new StringLiteral("2021-01-01")));
        List<Property> property = Lists.newArrayList();
        property.add(new Property("test", "test_value"));
        List<ListStringLiteral> stringLiterals = Lists.newArrayList();
        ListStringLiteral e = new ListStringLiteral(Lists.newArrayList(new StringLiteral("2021-01-01")));
        stringLiterals.add(e);
        e = new ListStringLiteral(Lists.newArrayList(new StringLiteral("2021-01-01"), new StringLiteral("2023-01-01")));
        stringLiterals.add(e);
        List<PartitionDesc> rangePartitons = Lists.newArrayList(
            new SingleItemListPartition(new Identifier("p1"), true, listStringLiteral, property),
            new MultiItemListPartition(new Identifier("p2"), false, stringLiterals, null)
        );
        ListPartitionedBy listPartitionedBy = new ListPartitionedBy(
            columns,
            rangePartitons
        );
        Boolean aBoolean = starRocksVisitor.visitListPartitionedBy(listPartitionedBy, 0);
        assertTrue(aBoolean);
        assertEquals("PARTITION BY LIST (k1)\n"
            + "(\n"
            + "PARTITION IF NOT EXISTS p1 VALUES IN (\"2021-01-01\") (\"test\"=\"test_value\"),\n"
            + "PARTITION p2 VALUES IN ((\"2021-01-01\"),(\"2021-01-01\",\"2023-01-01\"))\n"
            + ")", starRocksVisitor.getBuilder().toString());
    }

    private List<Property> toColumnProperties() {
        List<Property> properties = Lists.newArrayList();
        properties.add(new Property(StarRocksProperty.COLUMN_AGG_DESC.getValue(), AggDesc.SUM.name()));
        return properties;
    }

    private List<Property> toProperty() {
        List<Property> list = Lists.newArrayList();
        list.add(new Property(StarRocksProperty.TABLE_ENGINE.getValue(), "mysql"));
        return list;
    }

    private PartitionedBy toPartition() {
        List<ColumnDefinition> columns = Lists.newArrayList();
        columns.add(ColumnDefinition.builder().colName(new Identifier("c1")).build());
        columns.add(ColumnDefinition.builder().colName(new Identifier("c2")).build());
        List<PartitionDesc> rangePartition = Lists.newArrayList();
        LessThanPartitionKey partitionKey = new LessThanPartitionKey(false,
            new ListPartitionValue(Lists.newArrayList(new PartitionValue(false, new StringLiteral("2010-01-10")))));
        SingleRangePartition singleRangePartition = new SingleRangePartition(
            new Identifier("p1"), false, partitionKey, null);

        MultiRangePartition multiRangePartition = new MultiRangePartition(new StringLiteral("2001-01-01"), new StringLiteral("2020-01-01"),
            new IntervalLiteral(new LongLiteral("10"), DateTimeEnum.HOUR), null);
        rangePartition.add(singleRangePartition);
        rangePartition.add(multiRangePartition);
        return new RangePartitionedBy(
            columns, rangePartition
        );
    }

    private List<BaseConstraint> toConstraint() {
        PrimaryConstraint primaryConstraint = new PrimaryConstraint(
            IdentifierUtil.sysIdentifier(),
            Lists.newArrayList(new Identifier("c1"))
        );
        DuplicateConstraint duplicateConstraint = new DuplicateConstraint(
            IdentifierUtil.sysIdentifier(), Lists.newArrayList(new Identifier("c2")), true
        );

        AggregateConstraint aggregateConstraint = new AggregateConstraint(IdentifierUtil.sysIdentifier(), Lists.newArrayList(new Identifier("c2")));
        return Lists.newArrayList(primaryConstraint, duplicateConstraint, aggregateConstraint);
    }
}