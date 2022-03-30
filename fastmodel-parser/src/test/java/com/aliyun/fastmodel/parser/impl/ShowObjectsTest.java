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

import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.statement.constants.ShowObjectsType;
import com.aliyun.fastmodel.core.tree.statement.select.Limit;
import com.aliyun.fastmodel.core.tree.statement.select.Offset;
import com.aliyun.fastmodel.core.tree.statement.show.LikeCondition;
import com.aliyun.fastmodel.core.tree.statement.show.ShowObjects;
import com.aliyun.fastmodel.core.tree.statement.show.WhereCondition;
import com.aliyun.fastmodel.parser.BaseTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/12/2
 */
public class ShowObjectsTest extends BaseTest {

    @Test
    public void testValid() {
        String fml = "SHOW DERIVATIVE INDICATORS WHEREENGINE_TYPE='ODPS'";
        ShowObjects parse = parse(fml, ShowObjects.class);
        assertNotNull(parse);
    }

    @Test
    public void testShowObjects() {
        String sql = "show tables from dingtalk like 'abc%'";
        ShowObjects parse = parse(sql, ShowObjects.class);
        assertNotNull(parse);
        assertEquals(parse.getBaseUnit(), new Identifier("dingtalk"));
        assertEquals(parse.getConditionElement().getClass(), LikeCondition.class);
    }

    @Test
    public void testShowFullObjects() {
        String sql = "show full tables in dingtalk where name='abc'";
        ShowObjects showObject = parse(sql, ShowObjects.class);
        assertEquals(showObject.getConditionElement().getClass(), WhereCondition.class);
        assertEquals(showObject.getOrigin(), sql);
    }

    @Test
    public void testShowFullObjectsType() {
        String sql = "show full dim tables in dingtalk where name='abc'";
        ShowObjects showObject = parse(sql, ShowObjects.class);
        assertEquals(showObject.getConditionElement().getClass(), WhereCondition.class);
        assertEquals(showObject.getOrigin(), sql);
        ShowObjectsType showType = showObject.getShowType();
        assertEquals(showType, ShowObjectsType.DIM_TABLES);
    }

    @Test
    public void testShowIndicatorAtomic() {
        String s = "show atomic indicators in dingtalk like 'abc'";
        ShowObjects showObjects = parse(s, ShowObjects.class);
        assertEquals(showObjects.getShowType(), ShowObjectsType.ATOMIC_INDICATORS);
    }

    @Test
    public void testShowIndicatorAtomicComposite() {
        String s = "show atomic composite indicators; ";
        ShowObjects showObjects = parse(s, ShowObjects.class);
        assertEquals(showObjects.getShowType(), ShowObjectsType.ATOMIC_COMPOSITE_INDICATORS);
    }

    @Test
    public void testShowIndicatorDerivative() {
        String s = "show derivative indicators";
        ShowObjects showObjects = parse(s, ShowObjects.class);
        assertEquals(showObjects.getShowType(), ShowObjectsType.DERIVATIVE_INDICATORS);
    }

    @Test
    public void testShowIndicatorDerivativeComposite() {
        String s = "show derivative composite indicators";
        ShowObjects showObjects = parse(s, ShowObjects.class);
        assertEquals(showObjects.getShowType(), ShowObjectsType.DERIVATIVE_COMPOSITE_INDICATORS);
    }

    @Test
    public void testLayer() {
        String sql = "show layers;";
        ShowObjects showObjects = parse(sql, ShowObjects.class);
        assertEquals(showObjects.getShowType(), ShowObjectsType.LAYERS);
    }

    @Test
    public void testDicts() {
        String sql = "show dicts";
        ShowObjects showObjects = parse(sql, ShowObjects.class);
        assertEquals(showObjects.getShowType(), ShowObjectsType.DICTS);
    }

    @Test
    public void testMeasureUnitGroups() {
        String sql = "show measure_unit groups;";
        ShowObjects showObjects = parse(sql, ShowObjects.class);
        assertEquals(showObjects.getShowType(), ShowObjectsType.MEASURE_UNIT_GROUPS);
    }

    @Test
    public void testDictGroups() {
        String sql = "show dict groups;";
        ShowObjects showObjects = parse(sql, ShowObjects.class);
        assertEquals(showObjects.getShowType(), ShowObjectsType.DICT_GROUPS);
    }

    @Test
    public void testShowDomains() {
        String sql = "show domains;";
        ShowObjects showObjects = parse(sql, ShowObjects.class);
        assertEquals(showObjects.getShowType(), ShowObjectsType.DOMAINS);
    }

    @Test
    public void testAdjuncts() {
        String sql = "show adjuncts;";
        ShowObjects showObjects = parse(sql, ShowObjects.class);
        assertEquals(showObjects.getShowType(), ShowObjectsType.ADJUNCTS);
    }

    @Test
    public void testTimePeriods() {
        String sql = "show time_periods;";
        ShowObjects showObjects = parse(sql, ShowObjects.class);
        assertEquals(showObjects.getShowType(), ShowObjectsType.TIME_PERIODS);
    }

    @Test
    public void testShowNewLine() {
        String sql = "show\ntables";
        ShowObjects showObjects = parse(sql, ShowObjects.class);
        assertEquals(showObjects.getShowType(), ShowObjectsType.TABLES);
    }

    @Test
    public void showCodeTables() {
        String sql = "show code tables;";
        ShowObjects showObjects = parse(sql, ShowObjects.class);
        assertEquals(showObjects.getShowType(), ShowObjectsType.CODE_TABLES);
    }

    @Test
    public void showDimTables() {
        String sql = "show dim tables;";
        ShowObjects showObjects = parse(sql, ShowObjects.class);
        assertEquals(showObjects.getShowType(), ShowObjectsType.DIM_TABLES);
    }

    @Test
    public void showFactTables() {
        String sql = "show fact tables;";
        ShowObjects showObjects = parse(sql, ShowObjects.class);
        assertEquals(showObjects.getShowType(), ShowObjectsType.FACT_TABLES);
    }

    @Test
    public void showColumns() {
        String sql = "show columns from dim_shop";
        ShowObjects showObjects = parse(sql, ShowObjects.class);
        assertEquals(showObjects.getShowType(), ShowObjectsType.COLUMNS);
        assertEquals(showObjects.getTableName(), QualifiedName.of("dim_shop"));
    }

    @Test
    public void testShowTables() {
        String fml = "show dim tables\n"
            + "                  where detail_type = 'normal' \n"
            + "                  and column_keyword like '%xxx%'\n"
            + "                  offset 0 limit 100;";
        ShowObjects showObjects = parse(fml, ShowObjects.class);
        assertEquals(showObjects.getShowType(), ShowObjectsType.DIM_TABLES);
        Offset offset = showObjects.getOffset();
        assertEquals(offset.getRowCount(), new LongLiteral("0"));
        Limit limit = showObjects.getLimit();
        assertEquals(limit.getRowCount(), new LongLiteral("100"));
    }

    @Test
    public void testShowCodeTableContents() {
        String fml = "show codes from code_table_1";
        ShowObjects showObjects = parse(fml, ShowObjects.class);
        assertEquals(showObjects.getShowType(), ShowObjectsType.CODES);
        assertEquals(showObjects.getTableName(), QualifiedName.of("code_table_1"));
    }

    @Test
    public void testGroupCodeTableContents() {
        String fml = "show dict groups";
        ShowObjects showObjects = parse(fml, ShowObjects.class);
        assertEquals(showObjects.getShowType(), ShowObjectsType.DICT_GROUPS);
    }

    @Test
    public void testShowCategory() {
        String fml = "show business_categories;";
        ShowObjects showObjects = parse(fml, ShowObjects.class);
        assertEquals(showObjects.getShowType(), ShowObjectsType.BUSINESS_CATEGORIES);
    }

    @Test
    public void testShowDimensions() {
        String fml = "show dimensions;";
        ShowObjects showObjects = parse(fml, ShowObjects.class);
        assertEquals(showObjects.getShowType(), ShowObjectsType.DIMENSIONS);
    }

    @Test
    public void testShowMarkets() {
        String fml = "show markets;";
        ShowObjects showObjects = parse(fml, ShowObjects.class);
        assertEquals(showObjects.getShowType(), ShowObjectsType.MARKETS);
    }

    @Test
    public void testShowSubjects() {
        String fml = "show subjects;";
        ShowObjects showObjects = parse(fml, ShowObjects.class);
        assertEquals(showObjects.getShowType(), ShowObjectsType.SUBJECTS);
    }

    @Test
    public void testShowAttributes() {
        String fml = "show dim_attributes from a";
        ShowObjects showObjects = parse(fml, ShowObjects.class);
        assertEquals(showObjects.getShowType(), ShowObjectsType.DIM_ATTRIBUTES);
    }

    @Test
    public void testShowAttributesWithout() {
        String fml = "show dim_attributes like 'a'";
        ShowObjects showObjects = parse(fml, ShowObjects.class);
        assertEquals(showObjects.getShowType(), ShowObjectsType.DIM_ATTRIBUTES);
        assertEquals(showObjects.getConditionElement().getClass(), LikeCondition.class);
    }

    @Test
    public void testShowAdsTable() {
        String fml = "show ads tables;";
        ShowObjects showObjects = parse(fml, ShowObjects.class);
        assertEquals(showObjects.getShowType(), ShowObjectsType.ADS_TABLES);
    }

}
