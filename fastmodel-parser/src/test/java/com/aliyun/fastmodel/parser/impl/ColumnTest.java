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

import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.ChangeCol;
import com.aliyun.fastmodel.core.tree.statement.table.SetColProperties;
import com.aliyun.fastmodel.core.tree.statement.table.SetColumnOrder;
import com.aliyun.fastmodel.core.tree.statement.table.UnSetColProperties;
import com.aliyun.fastmodel.parser.BaseTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * 针对column修改的test
 *
 * @author panguanjing
 * @date 2021/3/9
 */
public class ColumnTest extends BaseTest {

    @Test
    public void setProperties() {
        String fml = "ALTER TABLE a.b CHANGE COLUMN col1 SET PROPERTIES('k1'='k2')";
        SetColProperties parse = parse(fml, SetColProperties.class);
        assertEquals(parse.getChangeColumn(), new Identifier("col1"));
        List<Property> properties = parse.getProperties();
        assertEquals(1, properties.size());
    }

    @Test
    public void testUnSetProperties() {
        String fml = "ALTER TABLE a.b CHANGE COLUMN col1 UNSET ('k1', 'k2')";
        UnSetColProperties unSetColProperties = parse(fml, UnSetColProperties.class);
        assertEquals(unSetColProperties.getChangeColumn(), new Identifier("col1"));
        assertEquals(unSetColProperties.getPropertyKeys().size(), 2);
    }

    @Test
    public void testChangeColumn() {
        String fml = "ALTER TABLE a.b CHANGE COLUMN col1 col1 BIGINT COMMENT 'comment' WITH ('key'='value')";
        ChangeCol changeCol = parse(fml, ChangeCol.class);
        assertEquals(changeCol.getColumnProperties().size(), 1);
    }

    @Test
    public void testColumnWithAliasedName() {
        String fml
            = "ALTER TABLE a.b CHANGE COLUMN col1 col1 ALIAS 'alias' BIGINT COMMENT 'comment' WITH ('key'='value')";
        ChangeCol changeCol = parse(fml, ChangeCol.class);
        assertEquals(changeCol.getColumnDefinition().getAliasedName().getName(), "alias");
    }

    @Test
    public void testChangeOrder() {
        String fml = "ALTER TABLE a.b CHANGE COLUMN c1 c2 BIGINT AFTER id";
        SetColumnOrder setColumnOrder = parse(fml, SetColumnOrder.class);
        assertEquals(setColumnOrder.getOldColName(), new Identifier("c1"));
        assertEquals(setColumnOrder.getNewColName(), new Identifier("c2"));
        assertEquals(setColumnOrder.getBeforeColName(), new Identifier("id"));

    }

    @Test
    public void testChangeOrder_first() {
        String fml = "ALTER TABLE a.b CHANGE COLUMN c1 c2 BIGINT FIRST";
        SetColumnOrder setColumnOrder = parse(fml, SetColumnOrder.class);
        assertEquals(setColumnOrder.getOldColName(), new Identifier("c1"));
        assertEquals(setColumnOrder.getNewColName(), new Identifier("c2"));
        assertEquals(setColumnOrder.getFirst(), true);
    }
}
