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

package com.aliyun.fastmodel.transform.oracle.parser;

import java.util.List;

import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.CompositeStatement;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.SetColComment;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableComment;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.DimConstraint;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Oracle Parser
 *
 * @author panguanjing
 * @date 2021/7/26
 */
public class OracleParserTest {
    OracleParser oracleParser = new OracleParser();

    @Test
    public void parseNode() {
        String dsl = "Create Table dim_shop (a int); comment on table dim_shop is '维度';";
        Node node = oracleParser.parseNode(dsl, ReverseContext.builder().build());
        CompositeStatement compositeStatement = (CompositeStatement)node;
        assertEquals(2, compositeStatement.getStatements().size());
    }

    @Test
    public void parseNodeSetComment() {
        String dsl = "comment on table dim_shop is '维度';";
        Node node = oracleParser.parseNode(dsl, ReverseContext.builder().build());
        SetTableComment setTableComment = (SetTableComment)node;
        assertEquals(setTableComment.getQualifiedName().getSuffix(), "dim_shop");
        assertEquals(setTableComment.getComment().getComment(), "维度");
    }

    @Test
    public void parseNodeSetColComment() {
        String node = "comment on column dim_shop.a is '字符串';";
        Node fml = oracleParser.parseNode(node);
        SetColComment setColComment = (SetColComment)fml;
        QualifiedName qualifiedName = setColComment.getQualifiedName();
        assertEquals(qualifiedName.toString(), "dim_shop");
    }

    @Test
    public void parseNodeDimConstraint() {
        String node = "create table dim_shop (a int, CONSTRAINT c1 FOREIGN KEY (a) REFERENCES dim_sku (a));";
        Node fmlNode = oracleParser.parseNode(node, ReverseContext.builder().build());
        CreateTable createTable = (CreateTable)fmlNode;
        List<BaseConstraint> constraintStatements = createTable.getConstraintStatements();
        DimConstraint dimConstraint = (DimConstraint)constraintStatements.get(0);
        assertEquals(dimConstraint.getColNames().size(), 1);
        List<Identifier> colNames = dimConstraint.getColNames();
        Identifier identifier = colNames.get(0);
        assertEquals(identifier.getValue(), "a");
    }
}