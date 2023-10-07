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

package com.aliyun.fastmodel.parser.expr;

import java.util.List;

import com.aliyun.fastmodel.core.parser.DomainLanguage;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.expr.ArithmeticBinaryExpression;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.BitOperationExpression;
import com.aliyun.fastmodel.core.tree.expr.ComparisonExpression;
import com.aliyun.fastmodel.core.tree.expr.IsConditionExpression;
import com.aliyun.fastmodel.core.tree.expr.LogicalBinaryExpression;
import com.aliyun.fastmodel.core.tree.expr.atom.Cast;
import com.aliyun.fastmodel.core.tree.expr.atom.Extract;
import com.aliyun.fastmodel.core.tree.expr.atom.Floor;
import com.aliyun.fastmodel.core.tree.expr.atom.FunctionCall;
import com.aliyun.fastmodel.core.tree.expr.atom.IntervalExpression;
import com.aliyun.fastmodel.core.tree.expr.atom.SearchedCaseExpression;
import com.aliyun.fastmodel.core.tree.expr.atom.SimpleCaseExpression;
import com.aliyun.fastmodel.core.tree.expr.atom.TableOrColumn;
import com.aliyun.fastmodel.core.tree.expr.atom.WhenClause;
import com.aliyun.fastmodel.core.tree.expr.enums.BitOperator;
import com.aliyun.fastmodel.core.tree.expr.enums.DateTimeEnum;
import com.aliyun.fastmodel.core.tree.expr.enums.IntervalQualifiers;
import com.aliyun.fastmodel.core.tree.expr.literal.BooleanLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.DecimalLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.ListStringLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.NullLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.TimestampLocalTzLiteral;
import com.aliyun.fastmodel.core.tree.expr.similar.BetweenPredicate;
import com.aliyun.fastmodel.core.tree.expr.similar.InListExpression;
import com.aliyun.fastmodel.core.tree.expr.similar.LikePredicate;
import com.aliyun.fastmodel.core.tree.expr.similar.NotExpression;
import com.aliyun.fastmodel.parser.NodeParser;
import org.hamcrest.Matchers;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * @author panguanjing
 * @date 2020/9/24
 */
public class ExpressionTest {

    private BaseExpression getBaseExpression(String expr) {
        NodeParser nodeParser = new NodeParser();
        return nodeParser.parseExpr(new DomainLanguage(expr));
    }

    @Test
    public void testExprAtomTableOrColumn() {
        String expr = "tableName";
        BaseExpression baseExpression = getBaseExpression(expr);
        assertEquals(baseExpression.getClass(), TableOrColumn.class);
        TableOrColumn tableOrColumn = (TableOrColumn)baseExpression;
        assertEquals(tableOrColumn.getQualifiedName(), QualifiedName.of("tablename"));
        assertEquals(expr, tableOrColumn.getOrigin());
    }

    @Test
    public void testExprAtomFunction() {
        String expr = "function(price)";
        BaseExpression baseExpression = getBaseExpression(expr);
        FunctionCall functionCall = (FunctionCall)baseExpression;
        QualifiedName functionName = functionCall.getFuncName();
        assertEquals(QualifiedName.of("function"), functionName);
        List<BaseExpression> selectExpressions = functionCall.getArguments();
        assertEquals(1, selectExpressions.size());
        assertEquals(functionCall.getOrigin(), expr);
    }

    @Test
    public void testExprAtomFunctionWithIdentifier() {
        String expr = "sum(tableName.field.field2)";
        BaseExpression baseExpression = getBaseExpression(expr);
        FunctionCall functionCall = (FunctionCall)baseExpression;
        assertEquals(functionCall.getFuncName(), QualifiedName.of("sum"));
    }

    @Test
    public void testExprAtomFunctionOperator() {
        String expr = "sum(table1 + table2)";
        equal(expr, FunctionCall.class);
    }

    @Test
    public void testExprAtomFloor() {
        String floor = "floor(abc TO YEAR)";
        BaseExpression baseExpression = getBaseExpression(floor);
        Floor compositeExpression = (Floor)baseExpression;
        assertEquals(compositeExpression.getFloorDateQualifiers(), DateTimeEnum.YEAR);
    }

    @Test
    public void testExprAtomWhen() {
        String whenExpr = "case when a then b"
            + " when c then d else e end";
        BaseExpression baseExpression = getBaseExpression(whenExpr);
        SearchedCaseExpression compositeExpression = (SearchedCaseExpression)baseExpression;
        assertNotNull(compositeExpression.getWhenClauseList());
        assertNotNull(compositeExpression.getDefaultValue());
    }

    @Test
    public void testCasWithPath() {
        String sql
            = "(case when a=1 "
            + "then price when a=2 then 1 end);";
        BaseExpression baseExpression = getBaseExpression(sql);
        SearchedCaseExpression searchedCaseExpression = (SearchedCaseExpression)baseExpression;
        List<WhenClause> whenClauseList = searchedCaseExpression.getWhenClauseList();
        assertEquals(2, whenClauseList.size());
    }

    @Test
    public void testExprAtomCase() {
        String caseExpr = "case abc when a then b"
            + " when c then d else e end";
        BaseExpression baseExpression = getBaseExpression(caseExpr);
        SimpleCaseExpression compositeExpression = (SimpleCaseExpression)baseExpression;
        assertNotNull(compositeExpression.getOperand());
        assertNotNull(compositeExpression.getWhenClauses());
        assertNotNull(compositeExpression.getDefaultValue());
    }

    @Test
    public void testExprAtomCast() {
        String cast = "cast(abc as bigint)";
        BaseExpression baseExpression = getBaseExpression(cast);
        Cast castExpression = (Cast)baseExpression;
        assertNotNull(cast);
        BaseDataType targetDataType = castExpression.getDataType();
        assertEquals(targetDataType.getTypeName(), DataTypeEnums.BIGINT);
    }

    @Test
    public void testExprSelectExpr() {
        String select = "function(*)";
        BaseExpression selectExpression = getBaseExpression(select);
        assertNotNull(selectExpression);
    }

    @Test
    public void testIsConditionExpr() {
        String condition = "abc is true";
        equal(condition, IsConditionExpression.class);
    }

    @Test
    public void testBitXorExpr() {
        String condition = "(abc ^ bcd)";
        BitOperationExpression comparisonExpression = (BitOperationExpression)getBaseExpression(condition);
        assertEquals(comparisonExpression.getOperator(), BitOperator.BITWISE_XOR);
        assertEquals(condition, comparisonExpression.toString());
    }

    @Test
    public void testStarExpr() {
        String startExpr = "(abc * bcd)";
        BaseExpression baseExpression = getBaseExpression(startExpr);
        ArithmeticBinaryExpression comparisonExpression = (ArithmeticBinaryExpression)baseExpression;
        assertNotNull(comparisonExpression);
        assertEquals(startExpr, comparisonExpression.toString());
    }

    @Test
    public void testPlusExpr() {
        String plusExpr = "(abc + bcd)";
        ArithmeticBinaryExpression comparisonExpression = (ArithmeticBinaryExpression)getBaseExpression(plusExpr);
        assertNotNull(comparisonExpression);
        assertEquals(plusExpr, comparisonExpression.toString());
    }

    @Test
    public void testConcatenateOpExpr() {
        String concatenate = "(abc || bcd)";
        BitOperationExpression comparisonExpression = (BitOperationExpression)getBaseExpression(concatenate);
        assertNotNull(comparisonExpression.getOperator());
        assertEquals(concatenate, comparisonExpression.toString());
    }

    @Test
    public void testAmpersandOpExpr() {
        String ampersand = "(abc & bcd)";
        BitOperationExpression comparisonExpression = (BitOperationExpression)getBaseExpression(ampersand);
        assertNotNull(comparisonExpression);
        assertEquals(ampersand, comparisonExpression.toString());
    }

    @Test
    public void testBitwiseOrExpr() {
        String bitwise = "(abc or bcd)";
        LogicalBinaryExpression comparisonExpression = (LogicalBinaryExpression)getBaseExpression(bitwise);
        assertNotNull(comparisonExpression);
        assertThat(bitwise, Matchers.equalToIgnoringCase(comparisonExpression.toString()));
    }

    @Test
    public void testSimilarExpr() {
        String similarExpr = "(abc >= bcd)";
        ComparisonExpression comparisonExpression = (ComparisonExpression)getBaseExpression(similarExpr);
        assertNotNull(comparisonExpression);
        assertEquals(similarExpr, comparisonExpression.toString());
    }

    @Test
    public void testEqualDistinctExpr() {
        String equalDistinct = "(abc is distinct from bcd)";
        ComparisonExpression comparisonExpression = (ComparisonExpression)getBaseExpression(equalDistinct);
        assertNotNull(comparisonExpression);
        assertThat(equalDistinct, Matchers.equalToIgnoringCase(comparisonExpression.toString()));
    }

    @Test
    public void testIsNotDistinct() {
        String isNotDistinct = "abc is not distinct from bcd";
        NotExpression comparisonExpression = (NotExpression)getBaseExpression(isNotDistinct);
        assertNotNull(comparisonExpression);
        assertEquals("NOT (abc IS DISTINCT FROM bcd)", comparisonExpression.toString());
    }

    @Test
    public void testIsNull() {
        String isNull = "abc is null";
        IsConditionExpression isConditionExpression = (IsConditionExpression)getBaseExpression(isNull);
        assertEquals("abc IS NULL", isConditionExpression.toString());
    }

    @Test
    public void testIsNotNull() {
        String isNotNull = "abc is not null";
        IsConditionExpression isConditionExpression = (IsConditionExpression)getBaseExpression(isNotNull);
        assertEquals("abc IS NOT NULL", isConditionExpression.toString());
    }

    @Test
    public void testIsFalse() {
        String isFalse = "abc is false";
        IsConditionExpression isConditionExpression = (IsConditionExpression)getBaseExpression(isFalse);
        assertEquals("abc IS FALSE", isConditionExpression.toString());
    }

    @Test
    public void testIsNotFalse() {
        String isFalse = "abc is not false";
        IsConditionExpression isConditionExpression = (IsConditionExpression)getBaseExpression(isFalse);
        assertEquals("abc IS NOT FALSE", isConditionExpression.toString());
    }

    @Test
    public void testIsNotTrue() {
        String isNotTrue = "abc is Not true";
        IsConditionExpression isConditionExpression = (IsConditionExpression)getBaseExpression(isNotTrue);
        assertEquals("abc IS NOT TRUE", isConditionExpression.toString());
    }

    @Test
    public void testAndExpr() {
        String andExpr = "(abc and bcd)";
        equal(andExpr, LogicalBinaryExpression.class);
    }

    @Test
    public void testOrExpr() {
        String orExpr = "(abc or bcd)";
        equal(orExpr, LogicalBinaryExpression.class);
    }

    @Test
    public void testVisitString() {
        String expr = "'StringText'";
        equal(expr, StringLiteral.class);
    }

    @Test
    public void testDayLiteral() {
        String expr = "(1234) DAY";
        equal(expr, IntervalExpression.class);
    }

    @Test
    public void testStringSequence() {
        String expr = "'123' '456'";
        equal(expr, ListStringLiteral.class);
    }

    @Test
    public void testIntegerLiteral() {
        String expr = "1234";
        BaseExpression baseExpression = getBaseExpression(expr);
        LongLiteral compositeExpression = (LongLiteral)baseExpression;
        assertEquals(compositeExpression.getValue(), new Long(1234L));
    }

    @Test
    public void testNumberLiteral() {
        String expr = "1234.1234";
        BaseExpression baseExpression = getBaseExpression(expr);
        DecimalLiteral compositeExpression = (DecimalLiteral)baseExpression;
        assertNotNull(compositeExpression);
    }

    @Test
    public void testCharSetLiteral() {
        String expr = "'_abc' 'abc'";
        BaseExpression baseExpression = getBaseExpression(expr);
        ListStringLiteral compositeExpression = (ListStringLiteral)baseExpression;
        assertNotNull(compositeExpression);
    }

    @Test
    public void testSimilarLikeExprExprInExpr() {
        String expr = "abc in (a, b, c)";
        BaseExpression baseExpression = getBaseExpression(expr);
        assertNotNull(baseExpression);
        assertTrue(baseExpression instanceof InListExpression);
        InListExpression inListExpression = (InListExpression)baseExpression;
        assertNotNull(inListExpression);
    }

    @Test
    public void testSimilarExprBetweenAndExpr() {
        String expr = "(expr BETWEEN 3 and 4)";
        equal(expr, BetweenPredicate.class);
    }

    @Test
    public void testSimilarExprLikeAny() {
        String expr = "(a like any (1, 2))";
        equal(expr, LikePredicate.class);
    }

    @Test
    public void testSimilarExprLikeAll() {
        String expr = "(a like all (1, 2))";
        equal(expr, LikePredicate.class);
    }

    @Test
    public void testExtract() {
        String expr = "extract(YEAR FROM 2)";
        equal(expr, Extract.class);
    }

    private void equal(String expr, Class<? extends BaseExpression> clazz) {
        BaseExpression baseExpression = getBaseExpression(expr);
        Object o = clazz.cast(baseExpression);
        assertThat(o.toString(), Matchers.equalToIgnoringCase(expr));
    }

    @Test
    public void testNull() {
        String expr = "NULL";
        equal(expr, NullLiteral.class);
    }

    @Test
    public void testTimeStampLocalTZLiteral() {
        String expr = "TIMESTAMPLOCALTZ '2020-01-01 11:00:00'";
        BaseExpression baseExpression = getBaseExpression(expr);
        assertNotNull(baseExpression);
        TimestampLocalTzLiteral compositeExpression = (TimestampLocalTzLiteral)baseExpression;
        assertNotNull(compositeExpression);
    }

    @Test
    public void testBooleanLiteral() {
        String expr = "TRUE";
        BaseExpression baseExpression = getBaseExpression(expr);
        assertNotNull(baseExpression);
        BooleanLiteral compositeExpression = (BooleanLiteral)baseExpression;
        assertNotNull(compositeExpression);
    }

    @Test
    public void testCompareExpression() {
        String expr = "a=b";
        BaseExpression baseExpression = getBaseExpression(expr);
        assertNotNull(baseExpression);
        assertEquals(expr, baseExpression.getOrigin());
    }

    @Test
    public void testCount() {
        String expr = "count(*)";
        equal(expr, FunctionCall.class);
    }

    @Test
    public void testInExpression() {
        String expr = "table_code.field1 in (1, 2)";
        BaseExpression baseExpression = getBaseExpression(expr);
        assertEquals(baseExpression.toString(), "table_code.field1 IN (1, 2)");
    }

    @Test
    public void testWindow() {
        String expr = "sum(a.b order by a desc) filter (where (a = 1)) over (range unbounded PRECEDING)";
        equal(expr, FunctionCall.class);
    }

    @Test
    public void testTimePeriod() {
        String date = "DATE_ADD(DATE \"2008-12-25\", INTERVAL 5 DAY)";
        BaseExpression baseExpression = getBaseExpression(date);
        assertNotNull(baseExpression);
        FunctionCall functionCall = (FunctionCall)baseExpression;
        List<BaseExpression> arguments = functionCall.getArguments();
        IntervalExpression baseExpression1 = (IntervalExpression)arguments.get(1);
        IntervalQualifiers intervalQualifiers = baseExpression1.getIntervalQualifiers();
        assertEquals(intervalQualifiers, IntervalQualifiers.DAY);
        assertEquals(baseExpression1.getIntervalValue(), new LongLiteral("5"));
    }

    @Test
    public void testDateAdd() {
        String expression = "DATE_ADD(DATE \"2008-12-25\", INTERVAL 5 WEEK,  'MO')\n";
        BaseExpression baseExpression = getBaseExpression(expression);
        FunctionCall functionCall = (FunctionCall)baseExpression;
        List<BaseExpression> arguments = functionCall.getArguments();
        BaseExpression baseExpression1 = arguments.get(2);
        assertEquals(baseExpression1, new StringLiteral("MO"));
    }

    @Test
    public void testFilter() {
        String expression = "dwd_fct_order.ds = '${bizate}'\n"
            + "\tAND dim_sku.ds = '${bizate}'\n"
            + "\tAND dim_shop.ds = '${bizate}'\n"
            + "\tAND dim_dept.ds = '${bizate}'\n"
            + "\tAND to_date(gmt_create, 'yyyy-mm-dd HH:mi:ss') >= to_date('${bizdate}', 'yyyymmdd')\n"
            + "\tAND to_date(gmt_create, 'yyyy-mm-dd HH:mi:ss') < dateadd(to_date('${bizdate}', 'yyyymmdd'), 1, 'dd')"
            + "\n";
        BaseExpression baseExpression = getBaseExpression(expression);
        assertEquals(
            "dwd_fct_order.ds = '${bizate}' AND dim_sku.ds = '${bizate}' AND dim_shop.ds = '${bizate}' "
                + "AND dim_dept.ds = '${bizate}' AND to_date(gmt_create, 'yyyy-mm-dd HH:mi:ss') >= to_date"
                + "('${bizdate}', 'yyyymmdd') AND to_date(gmt_create, 'yyyy-mm-dd HH:mi:ss') < dateadd(to_date"
                + "('${bizdate}', 'yyyymmdd'), 1, 'dd')",
            baseExpression.toString());
    }

    @Test
    public void testLogicExpression() {
        String logic = "a = 1 and b = 3 and c=4";
        BaseExpression baseExpression = getBaseExpression(logic);
        assertNotNull(baseExpression);
    }

    @Test
    public void testFloor() {
        String expression = "FLOOR (abc to YEAR)";
        Floor baseExpression = (Floor)getBaseExpression(expression);
        assertEquals(baseExpression.toString(), "FLOOR (abc TO YEAR)");
    }

    @Test
    public void testCaseWhen() {
        String fml =
            "CASE WHEN service_type_name RLIKE '工单' THEN ROW_NUMBER() OVER(PARTITION BY service_id,dealer_role ORDER "
                + "BY dealer_start_time ASC) END";
        BaseExpression baseExpression = getBaseExpression(fml);
        assertEquals(baseExpression.toString(),
            "(CASE WHEN service_type_name RLIKE '工单' THEN row_number() OVER (PARTITION BY service_id, dealer_role "
                + "ORDER BY dealer_start_time ASC) END)");

    }
}