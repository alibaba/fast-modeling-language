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

package com.aliyun.fastmodel.transform.mysql.parser;

import com.aliyun.fastmodel.core.exception.ParseException;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.statement.script.RefRelation;
import com.aliyun.fastmodel.core.tree.statement.table.AddConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.context.ReverseContext.ReverseTargetStrategy;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * MysqlTransformerParserTest
 *
 * @author panguanjing
 * @date 2021/9/3
 */
public class MysqlTransformerParserTest {

    MysqlTransformerParser mysqlTransformerParser = new MysqlTransformerParser();

    @Test
    public void testParser() {
        CreateTable createTable =
            (CreateTable)mysqlTransformerParser.parseNode(
                "create table if not exists a (b bigint unsigned comment 'comment');");
        assertEquals(createTable.toString(), "CREATE DIM TABLE IF NOT EXISTS a \n"
            + "(\n"
            + "   b BIGINT COMMENT 'comment'\n"
            + ")");
    }

    public void testParserEmptyColumn() {
        Node node = mysqlTransformerParser.parseNode("create table a");
        assertEquals(node.toString(), "CREATE TABLE a");
    }

    @Test
    public void testVisitCopy() {
        String ddl = "create table n_1 like o_1;";
        Node node = mysqlTransformerParser.parseNode(ddl);
        assertEquals(node.toString(), "CREATE DIM TABLE n_1 LIKE o_1");
    }

    @Test
    public void testTransform() {
        CreateTable node = (CreateTable)mysqlTransformerParser.parseNode(
            "CREATE TABLE empty_table \n(\n a BIGINT COMMENT 'comment'\n)"
                + " comment 'comment'");
        assertNotNull(node);
        assertTrue(node.isPropertyEmpty());
    }

    @Test
    public void testTransformEmptyColumn() {
        CreateTable node = (CreateTable)mysqlTransformerParser.parseNode("CREATE TABLE empty_table "
            + " comment 'comment'");
        assertNotNull(node);
        assertTrue(node.isPropertyEmpty());
    }

    @Test
    public void testTransformReverseScript() {
        Node node = mysqlTransformerParser.parseNode(
            "ALTER TABLE `a` ADD CONSTRAINT `name` FOREIGN KEY (`a`) REFERENCES `b` "
                + "(`a`); ", ReverseContext.builder().reverseTargetStrategy(ReverseTargetStrategy.SCRIPT).build());
        RefRelation relation = (RefRelation)node;
        assertEquals(relation.toString(), "REF `a`.`a` -> `b`.`a` : name");
    }

    @Test
    public void testTransformReverseDdl() {
        Node node = mysqlTransformerParser.parseNode(
            "ALTER TABLE `a` ADD CONSTRAINT `name` FOREIGN KEY (`a`) REFERENCES `b` "
                + "(`a`); ", ReverseContext.builder().reverseTargetStrategy(ReverseTargetStrategy.DDL).build());
        AddConstraint relation = (AddConstraint)node;
        assertEquals(relation.toString(), "ALTER TABLE `a` ADD CONSTRAINT `name` DIM KEY (`a`) REFERENCES b (`a`)");
    }

    @Test(expected = ParseException.class)
    public void testParseError() {
        String sql = "create table a (b bigint comment 'bac') coent 'bcd'; ";
        Node node = mysqlTransformerParser.parseNode(sql);
        assertNull(node);
    }
}