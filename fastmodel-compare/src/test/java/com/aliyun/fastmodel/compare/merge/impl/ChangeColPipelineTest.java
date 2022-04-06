/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.compare.merge.impl;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AliasedName;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.constants.TableDetailType;
import com.aliyun.fastmodel.core.tree.statement.table.ChangeCol;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2022/10/9
 */
public class ChangeColPipelineTest {
    ChangeColPipeline changeColPipeline = new ChangeColPipeline();

    @Test
    public void process() {
        CreateTable input = CreateTable.builder()
            .detailType(TableDetailType.ADS)
            .columns(
                ImmutableList.of(
                    ColumnDefinition.builder()
                        .colName(new Identifier("c1"))
                        .aliasedName(new AliasedName("c2"))
                        .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
                        .comment(new Comment("abc"))
                        .build()
                )
            ).build();
        ChangeCol baseStatement = new ChangeCol(
            QualifiedName.of("abc"),
            new Identifier("c1"),
            ColumnDefinition.builder()
                .colName(new Identifier("c1"))
                .aliasedName(new AliasedName("c2"))
                .build()
        );
        CreateTable process = changeColPipeline.process(input, baseStatement);
        List<ColumnDefinition> columnDefines = process.getColumnDefines();
        assertEquals(columnDefines.get(0).getCommentValue(), "abc");
        assertEquals(columnDefines.get(0).getAliasValue(), "c2");
    }

    @Test
    public void processPartition() {
        CreateTable input = CreateTable.builder()
            .detailType(TableDetailType.DWS)
            .columns(
                ImmutableList.of(
                    ColumnDefinition.builder()
                        .colName(new Identifier("c1"))
                        .aliasedName(new AliasedName("c2"))
                        .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
                        .comment(new Comment("abc"))
                        .build()
                )
            )
            .partition(new PartitionedBy(
                Lists.newArrayList(
                    ColumnDefinition.builder()
                        .colName(new Identifier("p1"))
                        .dataType(DataTypeUtil.simpleType(DataTypeEnums.BIGINT))
                        .build()
                )
            ))
            .build();
        ChangeCol baseStatement = new ChangeCol(
            QualifiedName.of("abc"),
            new Identifier("p1"),
            ColumnDefinition.builder()
                .colName(new Identifier("c1"))
                .aliasedName(new AliasedName("c2"))
                .comment(new Comment("abc"))
                .build()
        );
        CreateTable process = changeColPipeline.process(input, baseStatement);
        List<ColumnDefinition> columnDefines = process.getPartitionedBy().getColumnDefinitions();
        assertEquals(columnDefines.get(0).getCommentValue(), "abc");
        assertEquals(columnDefines.get(0).getAliasValue(), "c2");
    }
}