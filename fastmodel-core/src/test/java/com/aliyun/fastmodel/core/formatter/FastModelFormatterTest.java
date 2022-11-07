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

package com.aliyun.fastmodel.core.formatter;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AliasedName;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.expr.CustomExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.atom.TableOrColumn;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.constants.ColumnCategory;
import com.aliyun.fastmodel.core.tree.statement.constants.ShowObjectsType;
import com.aliyun.fastmodel.core.tree.statement.constants.ShowType;
import com.aliyun.fastmodel.core.tree.statement.constants.TableDetailType;
import com.aliyun.fastmodel.core.tree.statement.desc.Describe;
import com.aliyun.fastmodel.core.tree.statement.element.CreateElement;
import com.aliyun.fastmodel.core.tree.statement.element.MultiComment;
import com.aliyun.fastmodel.core.tree.statement.indicator.CreateAtomicCompositeIndicator;
import com.aliyun.fastmodel.core.tree.statement.indicator.CreateAtomicIndicator;
import com.aliyun.fastmodel.core.tree.statement.indicator.CreateDerivativeCompositeIndicator;
import com.aliyun.fastmodel.core.tree.statement.indicator.CreateDerivativeIndicator;
import com.aliyun.fastmodel.core.tree.statement.indicator.SetIndicatorProperties;
import com.aliyun.fastmodel.core.tree.statement.layer.Checker;
import com.aliyun.fastmodel.core.tree.statement.layer.CheckerType;
import com.aliyun.fastmodel.core.tree.statement.layer.CreateLayer;
import com.aliyun.fastmodel.core.tree.statement.select.Query;
import com.aliyun.fastmodel.core.tree.statement.select.item.SelectItem;
import com.aliyun.fastmodel.core.tree.statement.select.item.SingleColumn;
import com.aliyun.fastmodel.core.tree.statement.show.LikeCondition;
import com.aliyun.fastmodel.core.tree.statement.show.ShowObjects;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateCodeTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDimTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDwsTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateFactTable;
import com.aliyun.fastmodel.core.tree.statement.table.DropTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.RenameTable;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.ColumnGroupConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.DimConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.LevelConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.LevelDefine;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.core.tree.util.QueryUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * FastModelFormatterTest
 *
 * @author panguanjing
 * @date 2020/11/23
 */
public class FastModelFormatterTest {

    @Test
    public void format() {
        String abc = FastModelFormatter.formatNode(new StringLiteral("abc"));
        assertEquals(abc, "'abc'");
    }

    @Test
    public void testCreateTable() {
        List<Property> list = ImmutableList.of(new Property("business_process", "bp"), new Property("key1", "value1"));
        CreateDimTable root = CreateDimTable.builder().tableName(
            QualifiedName.of("a.b")).detailType(
            TableDetailType.LEVEL_DIM).ifNotExist(false).properties(list).build();
        String s = FastModelFormatter.formatNode(root);
        assertEquals("CREATE LEVEL DIM TABLE a.b\n"
            + "WITH('business_process'='bp','key1'='value1')", s);
    }

    @Test
    public void testCreateTableWithNormal() {
        List<Property> list = ImmutableList.of(new Property("business_process", "bp"), new Property("key1", "value1"));
        String s = FastModelFormatter.formatNode(CreateDimTable.builder().tableName(
            QualifiedName.of("a.b")).ifNotExist(false).properties(list).build());
        assertEquals("CREATE DIM TABLE a.b\n"
            + "WITH('business_process'='bp','key1'='value1')", s);
    }

    @Test
    public void testRenameTable() {
        String s = FastModelFormatter.formatNode(new RenameTable(QualifiedName.of("a.b"), QualifiedName.of("b.c")));
        assertEquals("ALTER TABLE a.b RENAME TO b.c", s);
    }

    @Test
    public void testCreateTableIfNotExists() {
        ImmutableList<ColumnDefinition> col = ImmutableList.of(
            ColumnDefinition.builder().colName(new Identifier("col"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.CHAR)).build());
        CreateDimTable createDimTable = CreateDimTable.builder().tableName(QualifiedName.of("a.b")).detailType(
            TableDetailType.TRANSACTION_FACT).comment(new Comment("comment")).columns(col).build();
        String s = FastModelFormatter.formatNode(createDimTable);
        assertEquals("CREATE FACT TABLE a.b \n"
            + "(\n"
            + "   col CHAR\n"
            + ")\n"
            + "COMMENT 'comment'", s);

    }

    @Test
    public void testCreateTableWithProperty() {
        DimConstraint dimConstraint = new DimConstraint(new Identifier("c1"),
            ImmutableList.of(new Identifier("a"), new Identifier("b")),
            QualifiedName.of("a.b"),
            ImmutableList.of(new Identifier("a"), new Identifier("b")));
        DimConstraint dimConstraint2 = new DimConstraint(
            new Identifier("c2"),
            ImmutableList.of(new Identifier("a"), new Identifier("b")),
            QualifiedName.of("b.d"),
            ImmutableList.of(new Identifier("f"), new Identifier("e")));
        ColumnDefinition element = ColumnDefinition.builder().colName(new Identifier("a")).dataType(
            DataTypeUtil.simpleType(DataTypeEnums.CHAR)).comment(new Comment("comment")).build();
        ColumnDefinition element2 = ColumnDefinition.builder().colName(
            new Identifier("b")).dataType(DataTypeUtil.simpleType(DataTypeEnums.VARCHAR)).comment(
            new Comment("comment")).build();
        CreateFactTable createDimTable =
            CreateFactTable.builder().tableName(
                    QualifiedName.of("a.b")).columns(
                    ImmutableList
                        .of(element, element2
                        )).constraints(ImmutableList.of(dimConstraint, dimConstraint2)).comment(new Comment("abc"))
                .properties(
                    ImmutableList.of(new Property("a", "b"))).build();
        String s = FastModelFormatter.formatNode(createDimTable);
        assertEquals("CREATE FACT TABLE a.b \n"
            + "(\n"
            + "   a CHAR COMMENT 'comment',\n"
            + "   b VARCHAR COMMENT 'comment',\n"
            + "   CONSTRAINT c1 DIM KEY (a,b) REFERENCES a.b (a,b),\n"
            + "   CONSTRAINT c2 DIM KEY (a,b) REFERENCES b.d (f,e)\n"
            + ")\n"
            + "COMMENT 'abc'\n"
            + "WITH('a'='b')", s);

    }

    @Test
    public void testCreateTableWithConstraintEmpty() {
        String result = FastModelFormatter.formatNode(CreateDimTable.builder().tableName(
            QualifiedName.of("a.b")).comment(new Comment("comment")).build());
        assertEquals("CREATE DIM TABLE a.b COMMENT 'comment'", result);
    }

    @Test
    public void testDropTable() {
        DropTable dropTable = new DropTable(QualifiedName.of("a.b"));
        String s = FastModelFormatter.formatNode(dropTable);
        assertEquals("DROP TABLE a.b", s);
    }

    @Test
    public void testCreateIndicator() {
        CreateElement createElement = CreateElement.builder().qualifiedName(QualifiedName.of("a.b")).comment(
            new Comment("comment")).notExists(true).build();
        CreateAtomicIndicator createAtomicIndicator = new CreateAtomicIndicator(
            createElement,
            DataTypeUtil.simpleType(DataTypeEnums.BIGINT),
            QueryUtil.functionCall("sum", new TableOrColumn(QualifiedName.of("a.b"))),
            null
        );
        String ddl = format(createAtomicIndicator);
        assertEquals("CREATE ATOMIC INDICATOR IF NOT EXISTS a.b BIGINT COMMENT 'comment'\n"
            + " AS sum(a.b)", ddl);
    }

    @Test
    public void testCreateAtomicComposite() {
        CreateElement createElement = CreateElement.builder().qualifiedName(QualifiedName.of("a.b")).comment(
            new Comment("comment")).build();
        CreateAtomicCompositeIndicator createAtomicCompositeIndicator = new CreateAtomicCompositeIndicator(
            createElement,
            DataTypeUtil.simpleType(DataTypeEnums.BIGINT),
            QueryUtil.functionCall("sum", new TableOrColumn(QualifiedName.of("a.b")))
        );
        String ddl = format(createAtomicCompositeIndicator);
        assertEquals("CREATE ATOMIC COMPOSITE INDICATOR a.b BIGINT COMMENT 'comment'\n"
            + " AS sum(a.b)", ddl);

    }

    @Test
    public void testCreateDerivative() {
        CreateElement createElement = CreateElement.builder().qualifiedName(QualifiedName.of("a.b")).properties(
            ImmutableList.of(new Property("a", "b"))
        ).comment(new Comment("comment")).build();
        CreateDerivativeIndicator createDerivativeIndicator = new CreateDerivativeIndicator(
            createElement,
            DataTypeUtil.simpleType(DataTypeEnums.BOOLEAN),
            null,
            QualifiedName.of("b.c")
        );
        String ddl = format(createDerivativeIndicator);
        assertEquals("CREATE DERIVATIVE INDICATOR a.b BOOLEAN REFERENCES b.c COMMENT 'comment' WITH ('a'='b')", ddl);

    }

    @Test
    public void testCreateDerivativeComposite() {
        ImmutableList<Property> of = ImmutableList.of(new Property("a", "b"));
        CreateElement createElement = CreateElement.builder().qualifiedName(QualifiedName.of("a.b")).properties(of)
            .comment(
                new Comment("comment")
            ).build();
        CreateDerivativeCompositeIndicator createDerivativeIndicator = new CreateDerivativeCompositeIndicator(
            createElement,
            DataTypeUtil.simpleType(DataTypeEnums.BOOLEAN),
            null,
            QualifiedName.of("b.c")
        );
        String ddl = format(createDerivativeIndicator);
        assertEquals(
            "CREATE DERIVATIVE COMPOSITE INDICATOR a.b BOOLEAN REFERENCES b.c COMMENT 'comment' WITH ('a'='b')", ddl);

    }

    @Test
    public void testCreateTableWithPartitionBy() {
        ColumnDefinition colName = ColumnDefinition.builder().colName(new Identifier("colName")).dataType(
            DataTypeUtil.simpleType(DataTypeEnums.BIGINT)
        ).comment(new Comment("字段_0")).category(ColumnCategory.MEASUREMENT).properties(
            ImmutableList.of(new Property("key1", "value1"))
        ).build();

        ColumnDefinition colName2 = ColumnDefinition.builder().colName(new Identifier("colName2")).dataType(
            DataTypeUtil.simpleType(DataTypeEnums.BIGINT)
        ).comment(new Comment("字段_0")).category(ColumnCategory.MEASUREMENT).properties(
            ImmutableList.of(new Property("key1", "value1"))
        ).build();

        ColumnDefinition colName3 = ColumnDefinition.builder().colName(new Identifier("colName3")).dataType(
            DataTypeUtil.simpleType(DataTypeEnums.BIGINT)
        ).comment(new Comment("字段_3")).category(ColumnCategory.MEASUREMENT).properties(
            ImmutableList.of(new Property("key1", "value1"))
        ).build();

        CreateDimTable createDimTable = CreateDimTable.builder().tableName(
                QualifiedName.of("a.b")).columns(
                ImmutableList.of(colName)).constraints(
                ImmutableList
                    .of(new ColumnGroupConstraint(new Identifier("c1"), ImmutableList.of(new Identifier("colName")))))
            .comment(
                new Comment("comment")
            ).properties(
                ImmutableList.of(new Property("key1", "value1"))).partition(
                new PartitionedBy(ImmutableList.of(colName2, colName3))
            ).build();

        String format = format(createDimTable);
        assertEquals(format, "CREATE DIM TABLE a.b \n"
            + "(\n"
            + "   colName BIGINT MEASUREMENT COMMENT '字段_0' WITH ('key1'='value1'),\n"
            + "   CONSTRAINT c1 COLUMN_GROUP (colName)\n"
            + ")\n"
            + "COMMENT 'comment'\n"
            + "PARTITIONED BY\n"
            + "(\n"
            + "   colName2 BIGINT MEASUREMENT COMMENT '字段_0' WITH ('key1'='value1'),\n"
            + "   colName3 BIGINT MEASUREMENT COMMENT '字段_3' WITH ('key1'='value1')\n"
            + ")\n"
            + "WITH('key1'='value1')");
    }

    @Test
    public void testCreateTableWithColumn() {
        ColumnDefinition colName = ColumnDefinition.builder().colName(new Identifier("colName"))
            .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT)).comment(
                new Comment("colComment")).category(
                ColumnCategory.MEASUREMENT
            ).properties(ImmutableList.of(new Property("key1", "value1"))).build();

        ColumnDefinition colName2 = ColumnDefinition.builder().colName(new Identifier("colName2")).dataType(
            DataTypeUtil.simpleType(DataTypeEnums.BIGINT)
        ).comment(new Comment("colComment")).category(ColumnCategory.MEASUREMENT).properties(
            ImmutableList.of(new Property("key1", "value1"))
        ).build();

        CreateDimTable createDimTable = CreateDimTable.builder().tableName(
                QualifiedName.of("a.b")).columns(
                ImmutableList.of(colName)).constraints(
                ImmutableList
                    .of(new ColumnGroupConstraint(new Identifier("c1"), ImmutableList.of(new Identifier("colName")))))
            .comment(
                new Comment("comment")
            ).properties(ImmutableList.of(new Property("key1", "value1"))).partition(
                new PartitionedBy(ImmutableList.of(colName2))
            ).build();
        String format = format(createDimTable);
        assertEquals("CREATE DIM TABLE a.b \n"
                + "(\n"
                + "   colName BIGINT MEASUREMENT COMMENT 'colComment' WITH ('key1'='value1'),\n"
                + "   CONSTRAINT c1 COLUMN_GROUP (colName)\n"
                + ")\n"
                + "COMMENT 'comment'\n"
                + "PARTITIONED BY\n"
                + "(\n"
                + "   colName2 BIGINT MEASUREMENT COMMENT 'colComment' WITH ('key1'='value1')\n"
                + ")\n"
                + "WITH('key1'='value1')"
            , format);
    }

    @Test
    public void testCreateLayer() {
        CreateElement createElement =
            CreateElement.builder().qualifiedName(
                QualifiedName.of("a.b")).comment(
                new Comment("comment")
            ).build();
        CreateLayer createLayer = new CreateLayer(createElement,
            ImmutableList.of(new Checker(CheckerType.REGEX, new StringLiteral("+d"), new Identifier("regex"),
                new Comment("comment"), true))
        );
        String format = format(createLayer);
        assertEquals("CREATE LAYER a.b( CHECKER regex regex '+d' COMMENT 'comment') COMMENT 'comment'", format);
    }

    @Test
    public void testShowObjects() {
        ShowObjects showObjects = new ShowObjects(
            new LikeCondition("%d"),
            true,
            new Identifier("database"),
            ShowObjectsType.DICTS
        );
        String format = format(showObjects);
        assertEquals("SHOW FULL DICTS FROM database LIKE '%d'", format);
    }

    private String format(Node node) {
        return FastModelFormatter.formatNode(node);
    }

    @Test
    public void testFormatLevelConstraint() {
        List<ColumnDefinition> columnDefines = Lists.newArrayList(
            ColumnDefinition.builder().colName(new Identifier("a"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT)).build()
        );

        List<LevelDefine> levelDefines =
            Lists.newArrayList(new LevelDefine(
                new Identifier("a"),
                Lists.newArrayList(new Identifier("a"))
            ));
        List<LevelDefine> levelDefines2 = Lists.newArrayList(new LevelDefine(
            new Identifier("a"), null)
        );
        List<BaseConstraint> constraints = Lists.newArrayList(
            new LevelConstraint(new Identifier("c1"), levelDefines, null),
            new LevelConstraint(new Identifier("c2"), levelDefines2, null)
        );
        CreateDimTable createDimTable = CreateDimTable.builder().tableName(
            QualifiedName.of("a.b")).detailType(
            TableDetailType.LEVEL_DIM
        ).columns(columnDefines).constraints(constraints).build();
        String format = format(createDimTable);
        assertEquals("CREATE LEVEL DIM TABLE a.b \n(\n"
            + "   a BIGINT,\n"
            + "   CONSTRAINT c1 LEVEL <a:(a)>,\n"
            + "   CONSTRAINT c2 LEVEL <a>\n"
            + ")", format);
    }

    @Test
    public void testQuery() {
        List<SelectItem> items = ImmutableList.of(
            new SingleColumn(new TableOrColumn(QualifiedName.of("a.b")), new Identifier("test")));
        Query query = QueryUtil.simpleQuery(QueryUtil.selectAll(items));
        String format = format(query);
        assertEquals("SELECT a.b test\n\n", format);

    }

    @Test
    public void testDesc() {
        Describe describe = new Describe(
            QualifiedName.of("a.b"),
            ShowType.TABLE
        );
        String format = format(describe);
        assertEquals("DESC TABLE a.b", format);
    }

    @Test
    public void testPartitionBy() {
        ColumnDefinition colName = ColumnDefinition.builder().colName(new Identifier("colName")).dataType(
            DataTypeUtil.simpleType(DataTypeEnums.BIGINT)
        ).comment(new Comment("字段_0")).category(ColumnCategory.MEASUREMENT).properties(
            ImmutableList.of(new Property("key1", "value1"))
        ).build();
        CreateDimTable createDimTable = CreateDimTable.builder().tableName(
            QualifiedName.of("a.b")).columns(
            ImmutableList.of(colName)).constraints(
            ImmutableList
                .of(new ColumnGroupConstraint(new Identifier("c1"), ImmutableList.of(new Identifier("colName"))))
        ).comment(
            new Comment("comment")
        ).properties(
            ImmutableList.of(new Property("key1", "value1"))
        ).partition(
            new PartitionedBy(null)
        ).createOrReplace(true).build();
        String format = format(createDimTable);
        assertEquals(format, "CREATE OR REPLACE DIM TABLE a.b \n"
            + "(\n"
            + "   colName BIGINT MEASUREMENT COMMENT '字段_0' WITH ('key1'='value1'),\n"
            + "   CONSTRAINT c1 COLUMN_GROUP (colName)\n"
            + ")\n"
            + "COMMENT 'comment'\n"
            + "WITH('key1'='value1')");
    }

    @Test
    public void testTableCommentElement() {
        MultiComment userId = new MultiComment(ColumnDefinition.builder().colName(new Identifier("user_id"))
            .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT)).build());
        CreateDimTable createDimTable = CreateDimTable.builder().tableName(QualifiedName.of("b")).insertColumnComment(
            new MultiComment(new StringLiteral("must exists"))
        ).insertColumnComment(
            userId
        ).insertColumnComment(userId).build();

        String format = format(createDimTable);
        assertEquals(format, "CREATE DIM TABLE b\n"
            + "/*(\n"
            + "   'must exists',\n"
            + "   user_id BIGINT,\n"
            + "   user_id BIGINT\n"
            + ")*/");

    }

    @Test
    public void testFormatAlias() {
        CreateDimTable build = CreateDimTable.builder().aliasedName(new AliasedName("abc")).tableName(
            QualifiedName.of("b")).build();
        String format = format(build);
        assertEquals(format, "CREATE DIM TABLE b ALIAS 'abc'");
    }

    @Test
    public void testSetIndicatorProperties() {
        SetIndicatorProperties setIndicatorProperties = new SetIndicatorProperties(
            QualifiedName.of("indicator_name"),
            ImmutableList.of(new Property("key1", "value1")),
            new CustomExpression("sum(1)"),
            null,
            DataTypeUtil.simpleType(DataTypeEnums.BIGINT)
        );
        String format = format(setIndicatorProperties);
        assertEquals(format, "ALTER INDICATOR indicator_name BIGINT SET PROPERTIES ('key1'='value1') AS sum(1)");
    }

    @Test
    public void testFormatDws() {
        CreateDwsTable build = CreateDwsTable.builder().detailType(TableDetailType.ADVANCED_DWS).tableName(
            QualifiedName.of("dws_demo")).build();
        String format = format(build);
        assertEquals("CREATE ADVANCED DWS TABLE dws_demo", format);
    }

    @Test
    public void testCodeTable() {
        CreateCodeTable code_test = CreateCodeTable.builder().tableName(QualifiedName.of("code_test")).build();
        String format = format(code_test);
        assertEquals("CREATE CODE TABLE code_test", format);
    }
}