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

package com.aliyun.fastmodel.parser.impl;

import java.util.List;

import com.aliyun.fastmodel.core.parser.DomainLanguage;
import com.aliyun.fastmodel.core.tree.AliasedName;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.atom.FunctionCall;
import com.aliyun.fastmodel.core.tree.expr.atom.TableOrColumn;
import com.aliyun.fastmodel.core.tree.expr.enums.VarType;
import com.aliyun.fastmodel.core.tree.statement.constants.IndicatorType;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import com.aliyun.fastmodel.core.tree.statement.indicator.CreateAtomicCompositeIndicator;
import com.aliyun.fastmodel.core.tree.statement.indicator.CreateAtomicIndicator;
import com.aliyun.fastmodel.core.tree.statement.indicator.CreateDerivativeCompositeIndicator;
import com.aliyun.fastmodel.core.tree.statement.indicator.CreateDerivativeIndicator;
import com.aliyun.fastmodel.core.tree.statement.indicator.CreateIndicator;
import com.aliyun.fastmodel.core.tree.statement.indicator.DropIndicator;
import com.aliyun.fastmodel.core.tree.statement.indicator.RenameIndicator;
import com.aliyun.fastmodel.core.tree.statement.indicator.SetIndicatorAlias;
import com.aliyun.fastmodel.core.tree.statement.indicator.SetIndicatorComment;
import com.aliyun.fastmodel.core.tree.statement.indicator.SetIndicatorProperties;
import com.aliyun.fastmodel.parser.NodeParser;
import com.aliyun.fastmodel.parser.visitor.AstExtractVisitor;
import org.hamcrest.MatcherAssert;
import org.junit.Test;

import static org.hamcrest.Matchers.equalToIgnoringWhiteSpace;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * 针对指标的测试内容
 *
 * @author panguanjing
 * @date 2020/9/22
 */
public class IndicatorTest {

    NodeParser fastModelAntlrParser = new NodeParser();

    @Test
    public void testAlterIndicatorExpr() {
        String sql = "alter indicator idc2 as count(*)";
        DomainLanguage domainLanguage = setDomainLanguage(sql);
        SetIndicatorProperties alterIndicatorExprStatement =
            (SetIndicatorProperties)fastModelAntlrParser.parse(domainLanguage);
        String identifier = alterIndicatorExprStatement.getIdentifier();
        assertEquals("idc2", identifier);
        assertEquals(alterIndicatorExprStatement.getStatementType(), StatementType.INDICATOR);
        FunctionCall newExpr = (FunctionCall)alterIndicatorExprStatement.getExpression();
        assertEquals(newExpr.getFuncName(), QualifiedName.of("count"));
    }

    @Test
    public void testAlterIndicatorExprAndReferences() {
        String sql = "alter indicator idc2 references tb1  as count(*)";
        DomainLanguage domainLanguage = setDomainLanguage(sql);
        SetIndicatorProperties alterIndicatorExprStatement =
            (SetIndicatorProperties)fastModelAntlrParser.parse(domainLanguage);
        QualifiedName references = alterIndicatorExprStatement.getReferences();
        assertEquals(references, QualifiedName.of("tb1"));
        assertEquals(alterIndicatorExprStatement.getStatementType(), StatementType.INDICATOR);
    }

    private DomainLanguage setDomainLanguage(String sql) {
        return new DomainLanguage(sql);
    }

    @Test
    public void testCreateIndicator() {
        DomainLanguage domainLanguage = new DomainLanguage(
            "create atomic indicator idc_name bigint comment 'indComment' as sum"
                + "(fact_table"
                + ".field)"
        );
        CreateIndicator createIndicatorStatement =
            (CreateIndicator)fastModelAntlrParser.parse(domainLanguage);
        assertEquals("idc_name", createIndicatorStatement.getIdentifier());
        assertEquals(new Comment("indComment"), createIndicatorStatement.getComment());
        assertEquals("sum(fact_table.field)", createIndicatorStatement.getIndicatorExpr().toString());
    }

    @Test
    public void testAlterIndicatorProperties() {
        String sql = "alter indicator idc2 set properties('key' = 'value')";
        DomainLanguage domainLanguage = setDomainLanguage(sql);
        SetIndicatorProperties propertiesStatement =
            (SetIndicatorProperties)fastModelAntlrParser.parse(domainLanguage);
        assertEquals(propertiesStatement.getIdentifier(), "idc2");
        List<Property> properties = propertiesStatement.getProperties();
        assertFalse(properties.isEmpty());
    }

    @Test
    public void testAlterIndicatorRename() {
        String sql = "alter indicator idc1 rename to idc2";
        RenameIndicator alterNameIndicatorStatement =
            (RenameIndicator)fastModelAntlrParser.parse(setDomainLanguage(sql));
        assertEquals(alterNameIndicatorStatement.getNewIdentifier(), "idc2");
        assertEquals(alterNameIndicatorStatement.getIdentifier(), "idc1");
    }

    @Test
    public void testCreateIndicatorStatement() {
        String indicator = "create atomic indicator idc1 bigint as sum(field1) ";
        CreateIndicator parse = (CreateIndicator)fastModelAntlrParser.parse(
            setDomainLanguage(indicator));
        assertEquals(parse.getIdentifier(), "idc1");
        FunctionCall indicatorExpr = (FunctionCall)parse.getIndicatorExpr();
        assertEquals("sum", indicatorExpr.getFuncName().toString());
        List<BaseExpression> selectExpressions = indicatorExpr.getArguments();
        BaseExpression selectExpression = selectExpressions.get(0);
        TableOrColumn atomExpression = (TableOrColumn)selectExpression;
        assertEquals(QualifiedName.of("field1"), atomExpression.getQualifiedName());
    }

    @Test
    public void testCreateIndicatorStar() {
        String indicator = "create atomic indicator idc1 bigint as sum(*) ";
        CreateIndicator parse = (CreateIndicator)fastModelAntlrParser.parse(
            setDomainLanguage(indicator));
        FunctionCall indicatorExpr = (FunctionCall)parse.getIndicatorExpr();
        assertEquals(QualifiedName.of("sum"), indicatorExpr.getFuncName());
    }

    @Test
    public void testCreateIndicatorWithExpr() {
        String sql = "CREATE INDICATOR idc_emp_count bigint  references dim_emp_info COMMENT '员工人数统计' WITH "
            + "PROPERTIES('type' = 'ATOMIC') AS count(1)\n";
        DomainLanguage domainLanguage = setDomainLanguage(sql);
        CreateAtomicIndicator parse = (CreateAtomicIndicator)fastModelAntlrParser.parse(domainLanguage);
        assertEquals(parse.getReferences(), QualifiedName.of("dim_emp_info"));
    }

    @Test
    public void testCreateWithType() {
        String sql = "CREATE ATOMIC INDICATOR idc_emp BIGINT COMMENT 'yuangong' AS COUNT(#abc#)";
        CreateAtomicIndicator createAtomicIndicator = (CreateAtomicIndicator)fastModelAntlrParser.parse(
            new DomainLanguage(sql));
        assertEquals(createAtomicIndicator.getComment(), new Comment("yuangong"));
    }

    @Test
    public void testCreateAtomicCompositeWithType() {
        String sql = "CREATE ATOMIC COMPOSITE INDICATOR idc_emp BOOLEAN";
        CreateAtomicCompositeIndicator compositeIndicator = (CreateAtomicCompositeIndicator)fastModelAntlrParser.parse(
            new DomainLanguage(sql));
        assertEquals(compositeIndicator.getQualifiedName(), QualifiedName.of("idc_emp"));
    }

    @Test
    public void testCreateDerivativeWithType() {
        String sql = "CREATE DERIVATIVE INDICATOR id_emp BIGINT REFERENCES a.b";
        CreateDerivativeIndicator derivativeIndicator = (CreateDerivativeIndicator)fastModelAntlrParser.parse(
            new DomainLanguage(sql));
        assertEquals(derivativeIndicator.getQualifiedName(), QualifiedName.of("id_emp"));
    }

    @Test
    public void testCreateDerivativeCompositeWithType() {
        String sql = "CREATE DERIVATIVE COMPOSITE INDICATOR id_emp bigint references a.b";
        CreateDerivativeCompositeIndicator compositeIndicator = (CreateDerivativeCompositeIndicator)fastModelAntlrParser
            .parse(new DomainLanguage(sql));
        assertEquals(compositeIndicator.getQualifiedName(), QualifiedName.of("id_emp"));
    }

    @Test
    public void testCreateIndicatorComplexExpr() {
        String sql
            = "CREATE atomic composite indicator ind_code BIGINT COMMENT '衍生字符串' AS "
            + "ind_1+id2";
        DomainLanguage domainLanguage = new DomainLanguage(sql);
        CreateIndicator parse = (CreateIndicator)fastModelAntlrParser.parse(domainLanguage);
        assertEquals(parse.getIdentifier(), "ind_code");
    }

    @Test
    public void testCreateIndicatorCaseWhen() {
        String sql
            = "create atomic composite indicator ind_code bigint AS (case when a=1 "
            + "then price when a=2 then 1 end);";
        DomainLanguage domainLanguage = new DomainLanguage(sql);
        CreateIndicator parse = (CreateIndicator)fastModelAntlrParser.parse(domainLanguage);
        BaseExpression indicatorExpr = parse.getIndicatorExpr();
        assertTrue(indicatorExpr.toString().contains(" "));

    }

    @Test
    public void testCreateDerivative() {
        String sql
            =
            "create derivative indicator ut.derivative_code bigint references atomic_indicator_code comment 'comment'"
                + " with "
                + "properties('dim'='fact_trade_pay', 'date_period' = 'week', 'filter_expr'= "
                + "'category=1', 'filter_name' = 'name')";
        DomainLanguage domainLanguage = new DomainLanguage(sql);
        CreateIndicator createIndicatorStatement = (CreateIndicator)fastModelAntlrParser.parse(
            domainLanguage);
        IndicatorType indicatorType = createIndicatorStatement.getIndicatorType();
        assertEquals(indicatorType, IndicatorType.DERIVATIVE);

    }

    @Test
    public void testComment() {
        String sql
            = "-- hello,world \r\n CREATE ATOMIC indicator id_code bigint references abc ";
        DomainLanguage domainLanguage = new DomainLanguage(sql);
        CreateIndicator createIndicatorStatement = (CreateIndicator)fastModelAntlrParser.parse(
            domainLanguage);
        assertEquals(createIndicatorStatement.getIdentifier(), "id_code");

    }

    @Test
    public void testRename() {
        String sql = "alter indicator id rename to u.id2";
        DomainLanguage domainLanguage = new DomainLanguage(sql);
        RenameIndicator createIndicatorStatement = (RenameIndicator)fastModelAntlrParser.parse(
            domainLanguage);
        assertEquals(createIndicatorStatement.getStatementType(), StatementType.INDICATOR);
        assertEquals("id2", createIndicatorStatement.getNewIdentifier());
    }

    @Test
    public void testAlterComposite() {
        String sql = "ALTER INDICATOR idc1 REFERENCES table1 SET PROPERTIES('key1'='value1') AS sum(a*c)";
        DomainLanguage domainLanguage = new DomainLanguage(sql);
        SetIndicatorProperties alterIndicatorCompositeStatement
            = (SetIndicatorProperties)fastModelAntlrParser.parse(domainLanguage);
        assertEquals(alterIndicatorCompositeStatement.getStatementType(), StatementType.INDICATOR);
        MatcherAssert.assertThat(alterIndicatorCompositeStatement.getExpression().toString(),
            equalToIgnoringWhiteSpace("sum(a * c)"));
        assertEquals(alterIndicatorCompositeStatement.getReferences(), QualifiedName.of("table1"));
        assertEquals(alterIndicatorCompositeStatement.getProperties().size(), 1);
    }

    @Test
    public void testCreateWithActual() {
        String sql = "CREATE DERIVATIVE COMPOSITE INDICATOR demo.shop_sku_1d_pay_price_avg DECIMAL\n"
            + "REFERENCES pay_price_avg\n"
            + "comment '门店&商品_近1天_生鲜门店&生鲜类目_平均支付金额'\n"
            + "WITH PROPERTIES(\n"
            + "      'bp_code' = 'default'\n"
            + ") AS (shop_sku_1d_pay_price_001 / shop_sku_1d_pay_count_001);";
        DomainLanguage domainLanguage = new DomainLanguage(sql);
        CreateIndicator createIndicatorStatement = (CreateIndicator)fastModelAntlrParser.parse(
            domainLanguage);
        assertEquals(createIndicatorStatement.getIdentifier(), "shop_sku_1d_pay_price_avg");
    }

    @Test
    public void testDropIndicator() {
        DomainLanguage domainLanguage = setDomainLanguage("drop indicator idc1");
        DropIndicator dropIndicatorStatement = (DropIndicator)fastModelAntlrParser.parse(
            domainLanguage);
        assertEquals(dropIndicatorStatement.getIdentifier(), "idc1");
        assertEquals(dropIndicatorStatement.getStatementType(), StatementType.INDICATOR);
    }

    @Test
    public void testCreateIndicatorWithSpace() {
        String sql = "create ATOMIC INDICATOR idc_code bigint as count(distinct order_id)";
        DomainLanguage domainLanguage = new DomainLanguage(sql);
        CreateIndicator createIndicatorStatement = (CreateIndicator)fastModelAntlrParser.parse(
            domainLanguage);
        BaseExpression indicatorExpr = createIndicatorStatement.getIndicatorExpr();
        FunctionCall functionCall = (FunctionCall)indicatorExpr;
        AstExtractVisitor astExtractVisitor = new AstExtractVisitor();
        functionCall.accept(astExtractVisitor, null);
        List<TableOrColumn> tableOrColumnList = astExtractVisitor.getTableOrColumnList();
        assertEquals(1, tableOrColumnList.size());
    }

    @Test
    public void testCreateIndicatorWithSingleQuote() {
        String sql
            = "create derivative indicator ut.idc1 bigint references abc with properties("
            + "'filter_expr'=\"abc='test'\")";
        CreateIndicator createIndicatorStatement = (CreateIndicator)fastModelAntlrParser.parse(
            new DomainLanguage(sql));
        List<Property> properties = createIndicatorStatement.getProperties();
        assertEquals(properties.size(), 1);
    }

    @Test
    public void testSetComment() {
        String setComment = "ALTER INDICATOR wq_bu.pay_count set comment '下单次数-V2';";
        SetIndicatorComment statement = (SetIndicatorComment)fastModelAntlrParser.parse(
            new DomainLanguage(setComment));
        assertNotNull(statement.getComment());
    }

    @Test
    public void testAlterTestCase() {
        String sql = "ALTER INDICATOR wq_bu.shop_sku_1d_pay_count_001 BIGINT\n"
            + "SET PROPERTIES(\n"
            + "      'filter_expr'= \"dim_shop.shop_type = '1' and dim_sku.cat_level_1_id = '2' \",\n"
            + "      'filter_name'= '生鲜门店&生鲜类目'\n"
            + ");";
        SetIndicatorProperties compositeStatement = (SetIndicatorProperties)fastModelAntlrParser
            .parse(new DomainLanguage(sql));
        assertEquals(compositeStatement.getIdentifier(), "shop_sku_1d_pay_count_001");
    }

    @Test
    public void testAlterTestCaseProperties() {
        String sql = "ALTER INDICATOR wq_bu.shop_sku_1d_pay_count_001\n"
            + "SET PROPERTIES(\n"
            + "      'filter_expr'= \"dim_shop.shop_type = '1' and dim_sku.cat_level_1_id = '2' \",\n"
            + "      'filter_name'= '生鲜门店&生鲜类目'\n"
            + ");";
        SetIndicatorProperties compositeStatement = (SetIndicatorProperties)fastModelAntlrParser
            .parse(new DomainLanguage(sql));
        assertEquals(compositeStatement.getIdentifier(), "shop_sku_1d_pay_count_001");
        assertNull(compositeStatement.getExpression());
    }

    @Test
    public void testCreateIndicatorWithVar() {
        String sql
            = "create atomic indicator idc bigint comment 'idc' as sum(${abc.price})";
        DomainLanguage domainLanguage = new DomainLanguage(sql);
        CreateIndicator createIndicatorStatement = (CreateIndicator)fastModelAntlrParser.parse(
            domainLanguage);
        BaseExpression indicatorExpr = createIndicatorStatement.getIndicatorExpr();
        FunctionCall functionCall = (FunctionCall)indicatorExpr;
        List<TableOrColumn> extract = fastModelAntlrParser.extract(new DomainLanguage(functionCall.toString()));
        assertEquals(extract.size(), 1);
        TableOrColumn tableOrColumn = extract.get(0);
        assertEquals(tableOrColumn.getVarType(), VarType.DOLLAR);
    }

    @Test
    public void testCreateIndicatorWithVar2() {
        String sql
            = "create atomic indicator idc bigint comment 'idc' as sum(#abc.price#)";
        DomainLanguage domainLanguage = new DomainLanguage(sql);
        CreateIndicator createIndicatorStatement = (CreateIndicator)fastModelAntlrParser.parse(
            domainLanguage);
        BaseExpression indicatorExpr = createIndicatorStatement.getIndicatorExpr();
        FunctionCall functionCall = (FunctionCall)indicatorExpr;
        List<TableOrColumn> extract = fastModelAntlrParser.extract(new DomainLanguage(functionCall.toString()));
        assertEquals(extract.size(), 1);
        TableOrColumn tableOrColumn = extract.get(0);
        assertSame(VarType.MACRO, tableOrColumn.getVarType());
        QualifiedName identifier = tableOrColumn.getQualifiedName();
        assertEquals(identifier.toString(), "abc.price");

    }

    @Test
    public void testCreateIndicatorWithNoPropertyKey() {
        String sql = "create atomic indicator idc bigint comment 'idc' with ('a'='b')";
        CreateAtomicIndicator indicator = (CreateAtomicIndicator)fastModelAntlrParser.parse(new DomainLanguage(sql));
        assertEquals(indicator.getProperties().size(), 1);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateAtomicIndicatorWithoutDataType() {
        String sql = "create atomic indicator idc comment 'idc'";
        CreateAtomicIndicator indicator = (CreateAtomicIndicator)fastModelAntlrParser.parse(new DomainLanguage(sql));
        assertNotNull(indicator);
    }

    @Test
    public void testCreateIndicatorWithoutDataType() {
        String sql = "create derivative indicator a.b references b.c comment 'abc' with('a' = 'b')";
        CreateDerivativeIndicator derivativeIndicator = (CreateDerivativeIndicator)fastModelAntlrParser.parse(
            new DomainLanguage(sql));
        assertNotNull(derivativeIndicator);
    }

    @Test
    public void testAlterIndicatorWithType() {
        String fml = "ALTER ATOMIC INDICATOR a.b RENAME TO a.c";
        RenameIndicator statement = fastModelAntlrParser.parseStatement(fml);
        assertEquals(statement.getNewIdentifier(), "c");
    }

    @Test
    public void testSetAlias() {
        String fml = "ALTER ATOMIC INDICATOR b SET ALIAS 'alias'";
        SetIndicatorAlias setIndicatorAlias = fastModelAntlrParser.parseStatement(fml);
        assertEquals(setIndicatorAlias.getAliasedName(), new AliasedName("alias"));
    }
}
