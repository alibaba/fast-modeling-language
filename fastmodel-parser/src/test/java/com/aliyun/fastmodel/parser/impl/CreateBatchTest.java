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

import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.batch.CreateBatchDomain;
import com.aliyun.fastmodel.core.tree.statement.batch.CreateBatchNamingDict;
import com.aliyun.fastmodel.core.tree.statement.batch.CreateIndicatorBatch;
import com.aliyun.fastmodel.core.tree.statement.batch.element.DateField;
import com.aliyun.fastmodel.core.tree.statement.batch.element.NamingDictElement;
import com.aliyun.fastmodel.parser.BaseTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * CreateBatchTest
 *
 * @author panguanjing
 * @date 2021/2/4
 */
public class CreateBatchTest extends BaseTest {
    @Test
    public void testCreateIndicator() {
        String sql
            = "create batch a.b (a comment 'abc' adjunct (a,b) references c as count(1), time_period c1, from table "
            + "(d, e), adjunct(d, e),dim table (f,g), dim path(<c,e>, <e,f>)) with ('k'='y');";

        CreateIndicatorBatch createBatch = parse(sql, CreateIndicatorBatch.class);
        assertNotNull(createBatch.getIndicatorDefine());
    }

    @Test
    public void testCreateIndicatorWithTwo() {
        String sql
            = "create batch bu_code.batch_code ("
            + "  der_code1 comment '派生1' adjunct (adjunct_code_2, adjunct_code_3) references atom_code1 as count(1)"
            + ", der_code2 comment '派生2' references atom_code2 as count(1)"
            + ", time_period d_1"
            + ", from table (fact_pay_order)"
            + ", dim table (dim_shop, dim_sku)"
            + ", date_field (a.field, 'yyyy-mm-dd')"
            + ") with ('k'='y');";
        CreateIndicatorBatch createBatch = parse(sql, CreateIndicatorBatch.class);
        assertEquals(createBatch.getIndicatorDefine().get(0).getCode(), new Identifier("der_code1"));
        DateField dateField = createBatch.getDateField();
        assertEquals(dateField.getPattern(), "yyyy-mm-dd");
    }

    @Test
    public void testReturnCreateBatch() {

        String sql
            = "CREATE BATCH DOMAIN bu_code.batch_code(a comment 'comment') COMMENT 'comment' WITH ('prop'='propkey')";

        CreateBatchDomain createBatchDomain = parse(sql, CreateBatchDomain.class);
        assertEquals(createBatchDomain.getDomainElements().size(), 1);

    }

    @Test
    public void testCreateBatchNamingDict() {
        String fml
            =
            "CREATE OR REPLACE BATCH NAMING DICT b (id alias 'alias' COMMENT 'comment', id2 alias 'alias' COMMENT "
                + "'comment2') "
                + "COMMENT 'comment' WITH ('key'='value')";
        CreateBatchNamingDict createBatchNamingDict = parse(fml, CreateBatchNamingDict.class);
        List<NamingDictElement> namingDictElementList = createBatchNamingDict.getNamingDictElementList();
        assertEquals(2, namingDictElementList.size());
        NamingDictElement dictElement = namingDictElementList.get(0);
        assertEquals(dictElement.getName(), new Identifier("id"));
        assertEquals(dictElement.getComment(), new Comment("comment"));
        Comment comment = createBatchNamingDict.getComment();
        assertEquals(comment, new Comment("comment"));
        List<Property> list = createBatchNamingDict.getProperties();
        assertEquals(1, list.size());
        Boolean createOrReplace = createBatchNamingDict.getCreateElement().getCreateOrReplace();
        assertTrue(createOrReplace);
    }

}
