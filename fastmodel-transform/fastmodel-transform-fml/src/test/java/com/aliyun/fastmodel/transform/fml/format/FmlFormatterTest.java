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

package com.aliyun.fastmodel.transform.fml.format;

import java.util.List;

import com.aliyun.fastmodel.core.parser.FastModelParser;
import com.aliyun.fastmodel.core.parser.FastModelParserFactory;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.CompositeStatement;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableComment;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.parser.lexer.ReservedIdentifier;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.fml.context.FmlTransformContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/8/22
 */
public class FmlFormatterTest {

    @Test
    public void testFmlVisitor() {
        List<ColumnDefinition> columns = getIncrease();
        CreateTable createTable = CreateTable.builder().columns(columns).tableName(QualifiedName.of("dim_shop"))
            .build();
        DialectNode dialectNode = FmlFormatter.formatNode(createTable, FmlTransformContext.builder().build());
        FastModelParser fastModelParser = FastModelParserFactory.getInstance().get();
        BaseStatement baseStatement = fastModelParser.parseStatement(dialectNode.getNode());
        assertNotNull(baseStatement.toString());
    }

    private List<ColumnDefinition> getIncrease() {
        List<ColumnDefinition> columnDefinitions = Lists.newArrayList();
        for (String keyword : ReservedIdentifier.getKeywords()) {
            ColumnDefinition build = ColumnDefinition.builder().colName(new Identifier(keyword)).dataType(
                DataTypeUtil.simpleType(DataTypeEnums.BIGINT)).build();
            columnDefinitions.add(build);
        }
        return columnDefinitions;
    }

    @Test
    public void testFml() {
        CompositeStatement compositeStatement = new CompositeStatement(
            ImmutableList.of(new SetTableComment(QualifiedName.of("dim_shop"), new Comment("comment1")),
                new SetTableComment(QualifiedName.of("dim_shop2"), new Comment("comment2"))));
        DialectNode dialectNode = FmlFormatter.formatNode(compositeStatement,
            FmlTransformContext.builder().appendSemicolon(true).build());
        assertEquals(dialectNode.getNode(),
            "ALTER TABLE dim_shop SET COMMENT 'comment1';\n" + "ALTER TABLE dim_shop2 SET COMMENT 'comment2';");
    }
}