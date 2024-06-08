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

import java.util.ArrayList;
import java.util.List;

import com.aliyun.fastmodel.core.tree.AliasedName;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.expr.ComparisonExpression;
import com.aliyun.fastmodel.core.tree.expr.CustomExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.Row;
import com.aliyun.fastmodel.core.tree.expr.atom.TableOrColumn;
import com.aliyun.fastmodel.core.tree.expr.enums.ComparisonOperator;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.adjunct.CreateAdjunct;
import com.aliyun.fastmodel.core.tree.statement.constants.ColumnCategory;
import com.aliyun.fastmodel.core.tree.statement.constants.ShowObjectsType;
import com.aliyun.fastmodel.core.tree.statement.constants.ShowType;
import com.aliyun.fastmodel.core.tree.statement.dict.CreateDict;
import com.aliyun.fastmodel.core.tree.statement.dqc.CreateDqcRule;
import com.aliyun.fastmodel.core.tree.statement.dqc.check.TableCheckElement;
import com.aliyun.fastmodel.core.tree.statement.element.CreateElement;
import com.aliyun.fastmodel.core.tree.statement.element.MultiComment;
import com.aliyun.fastmodel.core.tree.statement.group.CreateGroup;
import com.aliyun.fastmodel.core.tree.statement.group.GroupType;
import com.aliyun.fastmodel.core.tree.statement.indicator.CreateAtomicIndicator;
import com.aliyun.fastmodel.core.tree.statement.indicator.CreateDerivativeIndicator;
import com.aliyun.fastmodel.core.tree.statement.insert.Insert;
import com.aliyun.fastmodel.core.tree.statement.layer.DropLayer;
import com.aliyun.fastmodel.core.tree.statement.references.MoveReferences;
import com.aliyun.fastmodel.core.tree.statement.rule.AddRules;
import com.aliyun.fastmodel.core.tree.statement.rule.ChangeRuleElement;
import com.aliyun.fastmodel.core.tree.statement.rule.ChangeRules;
import com.aliyun.fastmodel.core.tree.statement.rule.CreateRules;
import com.aliyun.fastmodel.core.tree.statement.rule.DropRule;
import com.aliyun.fastmodel.core.tree.statement.rule.PartitionSpec;
import com.aliyun.fastmodel.core.tree.statement.rule.RuleDefinition;
import com.aliyun.fastmodel.core.tree.statement.rule.RuleGrade;
import com.aliyun.fastmodel.core.tree.statement.rule.RulesLevel;
import com.aliyun.fastmodel.core.tree.statement.rule.function.BaseFunctionName;
import com.aliyun.fastmodel.core.tree.statement.rule.function.VolFunction;
import com.aliyun.fastmodel.core.tree.statement.rule.function.column.ColumnFunction;
import com.aliyun.fastmodel.core.tree.statement.rule.function.table.TableFunction;
import com.aliyun.fastmodel.core.tree.statement.rule.strategy.DynamicStrategy;
import com.aliyun.fastmodel.core.tree.statement.rule.strategy.FixedStrategy;
import com.aliyun.fastmodel.core.tree.statement.rule.strategy.VolInterval;
import com.aliyun.fastmodel.core.tree.statement.rule.strategy.VolOperator;
import com.aliyun.fastmodel.core.tree.statement.rule.strategy.VolStrategy;
import com.aliyun.fastmodel.core.tree.statement.script.RefDirection;
import com.aliyun.fastmodel.core.tree.statement.script.RefObject;
import com.aliyun.fastmodel.core.tree.statement.script.RefRelation;
import com.aliyun.fastmodel.core.tree.statement.select.Query;
import com.aliyun.fastmodel.core.tree.statement.select.item.AllColumns;
import com.aliyun.fastmodel.core.tree.statement.select.item.SingleColumn;
import com.aliyun.fastmodel.core.tree.statement.show.ShowObjects;
import com.aliyun.fastmodel.core.tree.statement.show.WhereCondition;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.DropTable;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableAliasedName;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.NotNullConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.RedundantConstraint;
import com.aliyun.fastmodel.core.tree.statement.timeperiod.CreateTimePeriod;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.core.tree.util.IdentifierUtil;
import com.aliyun.fastmodel.core.tree.util.QueryUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * fast model visitor test
 *
 * @author panguanjing
 * @date 2021/5/19
 */
public class FastModelVisitorTest {

    private FastModelVisitor fastModelVisitor = new FastModelVisitor();

    @Test
    public void testVisitBaseSetAliasedName() {
        SetTableAliasedName setTableAliasedName = new SetTableAliasedName(QualifiedName.of("abc"),
            new AliasedName("alias"));
        fastModelVisitor.visitBaseSetAliasedName(setTableAliasedName, null);
        String s = fastModelVisitor.getBuilder().toString();
        assertEquals("ALTER TABLE abc SET ALIAS 'alias'", s);
    }

    @Test
    public void visitColumnFunction() {
        String a = ExpressionFormatter.formatExpression(
            new ColumnFunction(BaseFunctionName.DUPLICATE_COUNT, new TableOrColumn(QualifiedName.of("a")), null));
        assertEquals(a, "DUPLICATE_COUNT(a)");
    }

    @Test
    public void testCreateRules() {
        List<RuleDefinition> ruleDefine = ImmutableList.of(
            RuleDefinition.builder().ruleName(new Identifier("r1")).ruleGrade(RuleGrade.WEAK).ruleStrategy(
                new FixedStrategy(new TableFunction(BaseFunctionName.TABLE_SIZE, ImmutableList.of()),
                    ComparisonOperator.EQUAL,
                    new LongLiteral("0"))
            ).build()
        );
        CreateRules rules = CreateRules.builder().tableName(QualifiedName.of("dim_shop")).ruleLevel(RulesLevel.SQL)
            .ruleDefinitions(
                ruleDefine
            ).createElement(
                CreateElement.builder().qualifiedName(QualifiedName.of("abc")).build()).build();
        fastModelVisitor.visitCreateRules(rules, 1);
        StringBuilder builder = fastModelVisitor.getBuilder();
        assertEquals("CREATE SQL RULES abc REFERENCES dim_shop\n"
            + "(\n"
            + "      WEAK r1 TABLE_SIZE() = 0\n"
            + ")", builder.toString());
    }

    @Test
    public void testCreateRulesVolStrategy() {
        List<RuleDefinition> ruleDefine = ImmutableList.of(
            RuleDefinition.builder().ruleName(new Identifier("r1")).ruleGrade(RuleGrade.WEAK).ruleStrategy(
                new VolStrategy(new VolFunction(new ColumnFunction(
                    BaseFunctionName.UNIQUE_COUNT,
                    new TableOrColumn(QualifiedName.of("col1")),
                    DataTypeUtil.simpleType(DataTypeEnums.BIGINT)
                ), ImmutableList.of(new LongLiteral("1"))), VolOperator.INCREASE,
                    new VolInterval(new Integer(1), new Integer(2)))).build()
        );
        CreateRules rules = CreateRules.builder().tableName(QualifiedName.of("dim_shop")).ruleLevel(RulesLevel.SQL)
            .ruleDefinitions(
                ruleDefine
            ).createElement(
                CreateElement.builder().qualifiedName(QualifiedName.of("abc")).build()).build();
        fastModelVisitor.visitCreateRules(rules, 1);
        StringBuilder builder = fastModelVisitor.getBuilder();
        assertEquals("CREATE SQL RULES abc REFERENCES dim_shop\n"
            + "(\n"
            + "      WEAK r1 VOL(UNIQUE_COUNT(col1), 1) + [1,2]\n"
            + ")", builder.toString());
    }

    @Test
    public void testCreateRulesDyNamicStrategy() {
        List<RuleDefinition> ruleDefine = ImmutableList.of(
            RuleDefinition.builder().ruleName(new Identifier("r1")).
                ruleGrade(RuleGrade.WEAK).ruleStrategy(
                    new DynamicStrategy(
                        new ColumnFunction(
                            BaseFunctionName.UNIQUE_COUNT,
                            new TableOrColumn(QualifiedName.of("col1")),
                            DataTypeUtil.simpleType(DataTypeEnums.BIGINT)
                        ), new LongLiteral("15"))).build());
        List<PartitionSpec> list = ImmutableList.of(
            new PartitionSpec(new Identifier("col1"), new StringLiteral("yyyymmdd"))
        );
        CreateRules rules = CreateRules.builder().tableName(QualifiedName.of("dim_shop")).ruleLevel(RulesLevel.SQL)
            .ruleDefinitions(
                ruleDefine
            ).partitionSpecList(list).createElement(
                CreateElement.builder().qualifiedName(QualifiedName.of("abc")).build()).build();
        fastModelVisitor.visitCreateRules(rules, 1);
        StringBuilder builder = fastModelVisitor.getBuilder();
        assertEquals(builder.toString(), "CREATE SQL RULES abc REFERENCES dim_shop PARTITION (col1='yyyymmdd')\n"
            + "(\n"
            + "      WEAK r1 DYNMIAC(UNIQUE_COUNT(col1), 15)\n"
            + ")");
    }

    @Test
    public void testAddRules() {
        List<PartitionSpec> list = ImmutableList.of(
            new PartitionSpec(new Identifier("col1"), new StringLiteral("yyyymmdd")),
            new PartitionSpec(new Identifier("col2"), new StringLiteral("yyyymmdd"))
        );
        List<RuleDefinition> ruleDefine = ImmutableList.of(
            RuleDefinition.builder().ruleName(new Identifier("ruleName")).ruleStrategy(new FixedStrategy(
                new TableFunction(BaseFunctionName.TABLE_SIZE, ImmutableList.of()),
                ComparisonOperator.EQUAL,
                new LongLiteral("1")
            )).build(),
            RuleDefinition.builder().ruleGrade(RuleGrade.STRONG).ruleName(new Identifier("ru1")).comment(new Comment(
                "comment"
            )).build()
        );
        AddRules addRules = new AddRules(
            QualifiedName.of("table_name"),
            list,
            ruleDefine
        );
        fastModelVisitor.visitAddRules(addRules, 1);
        String s = fastModelVisitor.getBuilder().toString();
        assertEquals("ALTER RULES REFERENCES table_name PARTITION (col1='yyyymmdd',col2='yyyymmdd')\n"
            + "ADD RULE (\n"
            + "      WEAK ruleName TABLE_SIZE() = 1,\n"
            + "      STRONG ru1   COMMENT 'comment'\n"
            + ")", s);
    }

    @Test
    public void testAddRulesWithNoSetting() {
        List<PartitionSpec> list = ImmutableList.of(
            new PartitionSpec(new Identifier("col1"), null),
            new PartitionSpec(new Identifier("col2"), null)
        );
        List<RuleDefinition> ruleDefine = ImmutableList.of(
            RuleDefinition.builder().ruleName(new Identifier("ruleName")).ruleStrategy(new FixedStrategy(
                new TableFunction(BaseFunctionName.TABLE_SIZE, ImmutableList.of()),
                ComparisonOperator.EQUAL,
                new LongLiteral("1")
            )).build(),
            RuleDefinition.builder().ruleGrade(RuleGrade.STRONG).ruleName(new Identifier("ru1")).comment(new Comment(
                "comment"
            )).build()
        );
        AddRules addRules = new AddRules(
            QualifiedName.of("table_name"),
            list,
            ruleDefine
        );
        fastModelVisitor.visitAddRules(addRules, 1);
        String s = fastModelVisitor.getBuilder().toString();
        assertEquals("ALTER RULES REFERENCES table_name PARTITION (col1='[a-zA-Z0-9_-]*',col2='[a-zA-Z0-9_-]*')\n"
            + "ADD RULE (\n"
            + "      WEAK ruleName TABLE_SIZE() = 1,\n"
            + "      STRONG ru1   COMMENT 'comment'\n"
            + ")", s);
    }

    @Test
    public void testChangRules() {
        List<PartitionSpec> list = ImmutableList.of(
            new PartitionSpec(new Identifier("col1"), new StringLiteral("yyyymmdd")),
            new PartitionSpec(new Identifier("col2"), new StringLiteral("yyyymmdd"))
        );
        RuleDefinition ruleDefinition = RuleDefinition.builder().ruleName(new Identifier("ruleName")).ruleStrategy(
            new FixedStrategy(
                new TableFunction(BaseFunctionName.TABLE_SIZE, ImmutableList.of()),
                ComparisonOperator.EQUAL,
                new LongLiteral("1")
            )).build();
        ChangeRuleElement changeRuleElement = new ChangeRuleElement(
            new Identifier("c1"),
            ruleDefinition
        );
        List<ChangeRuleElement> ruleDefine = ImmutableList.of(
            changeRuleElement
        );
        ChangeRules changeRules = new ChangeRules(
            QualifiedName.of("dim_shop"),
            list,
            ruleDefine
        );
        fastModelVisitor.visitChangeRules(changeRules, 1);
        assertEquals("ALTER RULES REFERENCES dim_shop PARTITION (col1='yyyymmdd',col2='yyyymmdd')\n"
            + "CHANGE RULE (\n"
            + "c1      WEAK ruleName TABLE_SIZE() = 1\n)", fastModelVisitor.getBuilder().toString());
    }

    @Test
    public void testDropRule() {
        DropRule dropRule = new DropRule(
            RulesLevel.SQL,
            QualifiedName.of("tableName"),
            new Identifier("col1")
        );
        fastModelVisitor.visitDropRule(dropRule, 1);
        String s = fastModelVisitor.getBuilder().toString();
        assertEquals("ALTER SQL RULES tableName DROP RULE col1", s);

    }

    @Test
    public void testDropRuleWithout() {
        DropRule dropRule = new DropRule(
            null,
            QualifiedName.of("tableName"),
            new Identifier("col1")
        );
        fastModelVisitor.visitDropRule(dropRule, 1);
        String s = fastModelVisitor.getBuilder().toString();
        assertEquals("ALTER RULES tableName DROP RULE col1", s);
    }

    @Test
    public void testInsert() {
        Row row = new Row(ImmutableList.of(new StringLiteral("a")));
        Insert insert = new Insert(
            QualifiedName.of("dim_shop"),
            QueryUtil.query(QueryUtil.values(row)),
            ImmutableList.of(new Identifier("id"))
        );
        fastModelVisitor.visitInsert(insert, 1);
        String r = fastModelVisitor.getBuilder().toString();
        assertEquals(r, "INSERT INTO dim_shop (id) \n"
            + "  VALUES \n"
            + "     ('a')\n");
    }

    @Test
    public void testCreateDqcRule() {
        CreateDqcRule createDqcRule = CreateDqcRule.builder().tableName(QualifiedName.of("abc")).ruleDefinitions(
                ImmutableList.of(
                    TableCheckElement.builder()
                        .checkerName(new Identifier("checkName"))
                        .expression(new ComparisonExpression(
                            ComparisonOperator.EQUAL,
                            new TableOrColumn(QualifiedName.of("a"
                            )),
                            new LongLiteral("1")
                        ))
                        .build()
                )
            )
            .createElement(CreateElement.builder()
                .qualifiedName(QualifiedName.of("abc"))
                .build())
            .build();
        fastModelVisitor.visitCreateDqcRule(createDqcRule, 0);
        String s = fastModelVisitor.getBuilder().toString();
        assertNotNull(s);
    }

    @Test
    public void testCreateAtomic() {
        CreateAtomicIndicator createAtomicIndicator = new CreateAtomicIndicator(
            CreateElement.builder()
                .qualifiedName(QualifiedName.of("dim_shop"))
                .aliasedName(new AliasedName("alias"))
                .properties(ImmutableList.of(new Property("key", "value")))
                .build(),
            DataTypeUtil.simpleType(DataTypeEnums.BIGINT),
            null, null
        );
        fastModelVisitor.visitCreateAtomicIndicator(createAtomicIndicator, null);
        String s = fastModelVisitor.getBuilder().toString();
        assertEquals(s, "CREATE ATOMIC INDICATOR dim_shop ALIAS 'alias' BIGINT WITH ('key'='value')");
    }

    @Test
    public void testCreateDerivative() {
        CreateDerivativeIndicator createDerivativeIndicator = new CreateDerivativeIndicator(
            CreateElement.builder()
                .qualifiedName(QualifiedName.of("dim_shop"))
                .aliasedName(new AliasedName("alias"))
                .properties(ImmutableList.of(new Property("key", "value")))
                .build(),
            DataTypeUtil.simpleType(DataTypeEnums.BIGINT),
            null, QualifiedName.of("atomic_indicator")
        );
        fastModelVisitor.visitCreateDerivativeIndicator(createDerivativeIndicator, null);
        String s = fastModelVisitor.getBuilder().toString();
        assertEquals(s,
            "CREATE DERIVATIVE INDICATOR dim_shop ALIAS 'alias' BIGINT REFERENCES atomic_indicator WITH "
                + "('key'='value')");
    }

    @Test
    public void testCreateDerivativeWithoutDataType() {
        CreateDerivativeIndicator createDerivativeIndicator = new CreateDerivativeIndicator(
            CreateElement.builder()
                .qualifiedName(QualifiedName.of("dim_shop"))
                .aliasedName(new AliasedName("alias"))
                .properties(ImmutableList.of(new Property("key", "value")))
                .build(),
            null,
            null, QualifiedName.of("atomic_indicator")
        );
        fastModelVisitor.visitCreateDerivativeIndicator(createDerivativeIndicator, null);
        String s = fastModelVisitor.getBuilder().toString();
        assertEquals(s,
            "CREATE DERIVATIVE INDICATOR dim_shop ALIAS 'alias' REFERENCES atomic_indicator WITH ('key'='value')");
    }

    @Test
    public void testCreateTimePeriod() {
        CreateTimePeriod createTimePeriod = new CreateTimePeriod(
            CreateElement.builder()
                .qualifiedName(QualifiedName.of("name"))
                .aliasedName(new AliasedName("alias"))
                .comment(new Comment("comment"))
                .build(),
            null
        );
        fastModelVisitor.visitCreateTimePeriod(createTimePeriod, null);
        String s = fastModelVisitor.getBuilder().toString();
        assertEquals(s, "CREATE TIME_PERIOD name ALIAS 'alias' COMMENT 'comment'");
    }

    @Test
    public void testInsertValues() {
        Query query = QueryUtil.query(QueryUtil.values(new Row(ImmutableList.of(
            new StringLiteral("1")
        ))));
        Insert insert = new Insert(
            true
            , QualifiedName.of("dim_shop")
            , null
            , query
            , ImmutableList.of(new Identifier("a")));
        fastModelVisitor.visitInsert(insert, 0);
        String s = fastModelVisitor.getBuilder().toString();
        assertEquals(s, "INSERT OVERWRITE dim_shop VALUES \n"
            + "  ('1')\n");
    }

    @Test
    public void testCreateDict() {
        CreateDict createDict = new CreateDict(CreateElement.builder()
            .qualifiedName(QualifiedName.of("abc"))
            .aliasedName(new AliasedName("alias"))
            .comment(new Comment("comment"))
            .properties(ImmutableList.of(new Property("code", "value")))
            .build()
            , DataTypeUtil.simpleType(DataTypeEnums.BIGINT),
            new NotNullConstraint(IdentifierUtil.sysIdentifier(), true),
            new StringLiteral("abc")
        );
        fastModelVisitor.visitCreateDict(createDict, 0);
        String s = fastModelVisitor.getBuilder().toString();
        assertEquals(s, "CREATE DICT abc ALIAS 'alias' BIGINT NOT NULL DEFAULT 'abc' COMMENT 'comment'\n"
            + "WITH('code'='value')");
    }

    @Test
    public void testCreateGroup() {
        CreateGroup createGroup = new CreateGroup(
            CreateElement.builder()
                .qualifiedName(QualifiedName.of("ddd"))
                .comment(new Comment("comment"))
                .build(),
            GroupType.CODE
        );
        fastModelVisitor.visitCreateGroup(createGroup, 0);

        String b = fastModelVisitor.getBuilder().toString();
        assertEquals(b, "CREATE GROUP CODE ddd COMMENT 'comment'");
    }

    @Test
    public void testVisitComposite() {
        String a = "a;\nb;\nc;";
        String[] split = StringUtils.split(a, ";\n");
        System.out.println(Lists.newArrayList(split));
        String b = "a";
        split = StringUtils.split(b, ";\n");
        System.out.println(Lists.newArrayList(split));
    }

    @Test
    public void dropTable() {
        DropTable dropTable = new DropTable(QualifiedName.of("dim_shop"));
        fastModelVisitor.visitDropTable(dropTable, 0);
        assertEquals(fastModelVisitor.getBuilder().toString(), "DROP TABLE dim_shop");
        dropTable = new DropTable(QualifiedName.of("dim_shop"), true);
        fastModelVisitor = new FastModelVisitor();
        fastModelVisitor.visitDropTable(dropTable, 0);
        assertEquals(fastModelVisitor.getBuilder().toString(), "DROP TABLE IF EXISTS dim_shop");
    }

    @Test
    public void testDropLayer() {
        DropLayer dropLayer = new DropLayer(QualifiedName.of("abc"));
        fastModelVisitor.visitDropLayer(dropLayer, 0);
        assertEquals(fastModelVisitor.getBuilder().toString(), "DROP LAYER abc");
    }

    @Test
    public void testFormatColumnDefinition() {
        ColumnDefinition definition = ColumnDefinition.builder()
            .colName(new Identifier("c1"))
            .category(ColumnCategory.ATTRIBUTE)
            .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
            .build();
        fastModelVisitor.visitColumnDefine(definition, 0);
        String s = fastModelVisitor.getBuilder().toString();
        assertEquals(s, "c1 BIGINT");
    }

    @Test
    public void testFormatColumnDefinition_correlation() {
        ColumnDefinition definition = ColumnDefinition.builder()
            .colName(new Identifier("c1"))
            .category(ColumnCategory.CORRELATION)
            .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
            .build();
        fastModelVisitor.visitColumnDefine(definition, 0);
        String s = fastModelVisitor.getBuilder().toString();
        assertEquals(s, "c1 BIGINT CORRELATION");
    }

    @Test
    public void testVisitMulitComment() {
        MultiComment comment = new MultiComment(
            ColumnDefinition.builder()
                .colName(new Identifier("abc"))
                .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
                .build()
        );
        fastModelVisitor.visitMultiComment(comment, 0);
        String s = fastModelVisitor.getBuilder().toString();
        assertEquals(s, "/*\n"
            + "   abc BIGINT\n"
            + "*/");
    }

    @Test
    public void testVisitShow() {
        ShowObjects showObjects = new ShowObjects(
            null, true, null, ShowObjectsType.TABLES
        );
        fastModelVisitor.visitShowObjects(showObjects, 0);
        String s = fastModelVisitor.getBuilder().toString();
        assertEquals("SHOW FULL TABLES", s);
    }

    @Test
    public void testVisitShowCondition() {
        ShowObjects showObjects = new ShowObjects(
            new WhereCondition(new CustomExpression("a='b'")), true, null, ShowObjectsType.TABLES
        );
        fastModelVisitor.visitShowObjects(showObjects, 0);
        String s = fastModelVisitor.getBuilder().toString();
        assertEquals("SHOW FULL TABLES WHERE a='b'", s);
    }

    @Test
    public void testVisitShowConditionFrom() {
        ShowObjects showObjects = new ShowObjects(
            new WhereCondition(new CustomExpression("a='b'")), true, new Identifier("abc"), ShowObjectsType.TABLES
        );
        fastModelVisitor.visitShowObjects(showObjects, 0);
        String s = fastModelVisitor.getBuilder().toString();
        assertEquals("SHOW FULL TABLES FROM abc WHERE a='b'", s);
    }

    @Test
    public void testVisitShowConditionFromColumn() {
        ShowObjects showObjects = new ShowObjects(
            null, new Identifier("a"), ShowObjectsType.COLUMNS,
            Lists.newArrayList(QualifiedName.of("dim_shop"))
        );
        fastModelVisitor.visitShowObjects(showObjects, 0);
        String s = fastModelVisitor.getBuilder().toString();
        assertEquals("SHOW COLUMNS FROM dim_shop", s);
    }

    @Test
    public void testColumnWithEmpty() {
        Node commentNode = ColumnDefinition
            .builder()
            .colName(new Identifier("c"))
            .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
            .build();
        CreateTable node = CreateTable.builder()
            .tableName(QualifiedName.of("dim_shop"))
            .insertColumnComment(new MultiComment(
                commentNode
            ))
            .build();
        fastModelVisitor.visitCreateTable(node, 0);
        String s = fastModelVisitor.getBuilder().toString();
        assertEquals(s, "CREATE TABLE dim_shop\n"
            + "/*(\n"
            + "   c BIGINT\n"
            + ")*/");
    }

    @Test
    public void testFormateRefRelation() {
        ArrayList<Identifier> abc = Lists.newArrayList(new Identifier("abc"));
        RefObject left = new RefObject(QualifiedName.of("a"), abc, new Comment("comment"));
        RefObject right = new RefObject(QualifiedName.of("b"), Lists.newArrayList(new Identifier("bcd")),
            new Comment("de"));
        RefRelation relation = new RefRelation(QualifiedName.of("abc"), left, right, RefDirection.LEFT_DIRECTION_RIGHT);
        fastModelVisitor.visitRefEntityStatement(relation, 0);
        StringBuilder builder = fastModelVisitor.getBuilder();
        assertEquals(builder.toString(), "REF a.abc COMMENT 'comment' -> b.bcd COMMENT 'de' : abc");
    }

    @Test
    public void testVisitMoveReferences() {
        MoveReferences references = new MoveReferences(
            ShowType.DOMAIN,
            QualifiedName.of("a.b"),
            QualifiedName.of("b.c"),
            null
        );
        fastModelVisitor.visitMoveReferences(references, 0);
        String text = fastModelVisitor.getBuilder().toString();
        assertEquals(text, "MOVE DOMAIN REFERENCES a.b TO b.c");
    }

    @Test
    public void testCreatAdjunct() {
        CreateAdjunct createAdjunct = new CreateAdjunct(
            CreateElement.builder()
                .createOrReplace(true)
                .qualifiedName(QualifiedName.of("abc"))
                .comment(new Comment("comment"))
                .build(),
            new StringLiteral("abc")
        );
        fastModelVisitor.visitCreateAdjunct(createAdjunct, 0);
        assertEquals(fastModelVisitor.getBuilder().toString(), "CREATE OR REPLACE ADJUNCT abc COMMENT 'comment' AS 'abc'");
    }

    @Test
    public void testVisitSingleColumn() {
        SingleColumn singleColumn = new SingleColumn(
            null, new TableOrColumn(QualifiedName.of("abc")), new Identifier("a"), true
        );
        fastModelVisitor.visitSingleColumn(singleColumn, 0);
        assertEquals(fastModelVisitor.getBuilder().toString(), "abc AS a");
    }

    @Test
    public void testVisitSingleColumnWithoutAs() {
        SingleColumn singleColumn = new SingleColumn(
            null, new TableOrColumn(QualifiedName.of("abc")), new Identifier("a"), false
        );
        fastModelVisitor.visitSingleColumn(singleColumn, 0);
        assertEquals(fastModelVisitor.getBuilder().toString(), "abc a");
    }

    @Test
    public void testVisitAllColumnWithoutAs() {
        AllColumns singleColumn = new AllColumns(
            null, new TableOrColumn(QualifiedName.of("abc")), ImmutableList.of(new Identifier("a")), true
        );
        fastModelVisitor.visitAllColumns(singleColumn, 0);
        assertEquals(fastModelVisitor.getBuilder().toString(), "abc.* AS (a)");
    }

    @Test
    public void testVisitAllColumnWithAs() {
        AllColumns singleColumn = new AllColumns(
            null, new TableOrColumn(QualifiedName.of("abc")), ImmutableList.of(new Identifier("a")), false
        );
        fastModelVisitor.visitAllColumns(singleColumn, 0);
        assertEquals(fastModelVisitor.getBuilder().toString(), "abc.* (a)");
    }

    @Test
    public void testVisitRedunctConstraint() {

        RedundantConstraint constraint = new RedundantConstraint(
            new Identifier("c1"),
            new Identifier("abc"),
            QualifiedName.of("abc.bcd"),
            null
        );
        fastModelVisitor.visitRedundantConstraint(constraint, 0);
        String s = fastModelVisitor.getBuilder().toString();
        assertEquals(s, "CONSTRAINT c1 REDUNDANT abc REFERENCES abc.bcd");
    }
    @Test
    public void testVisitRedunctConstraint2() {
        RedundantConstraint constraint = new RedundantConstraint(
            new Identifier("c1"),
            new Identifier("abc"),
            QualifiedName.of("abc.bcd.$"),
            null
        );
        fastModelVisitor.visitRedundantConstraint(constraint, 0);
        String s = fastModelVisitor.getBuilder().toString();
        assertEquals(s, "CONSTRAINT c1 REDUNDANT abc REFERENCES abc.bcd.`$`");
    }

    @Test
    public void testVisitRedunctConstraint3() {
        RedundantConstraint constraint = new RedundantConstraint(
           IdentifierUtil.sysIdentifier(),
            new Identifier("abc"),
            QualifiedName.of("abc.bcd.$"),
            Lists.newArrayList(new Identifier("c2"), new Identifier("c3"))
        );
        fastModelVisitor.visitRedundantConstraint(constraint, 0);
        String s = fastModelVisitor.getBuilder().toString();
        assertEquals(s, "REDUNDANT abc REFERENCES abc.bcd.`$`(c2,c3)");
    }
}