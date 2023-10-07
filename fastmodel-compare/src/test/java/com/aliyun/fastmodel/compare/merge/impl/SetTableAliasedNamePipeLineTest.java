/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.compare.merge.impl;

import com.aliyun.fastmodel.core.tree.AliasedName;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.constants.TableDetailType;
import com.aliyun.fastmodel.core.tree.statement.table.CreateAdsTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableAliasedName;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2022/10/9
 */
public class SetTableAliasedNamePipeLineTest {
    SetTableAliasedNamePipeLine setTableAliasedNamePipeLine = new SetTableAliasedNamePipeLine();

    @Test
    public void testProcess() {
        CreateTable input = CreateTable.builder()
            .detailType(TableDetailType.ADS)
            .aliasedName(new AliasedName("a1"))
            .build();
        CreateTable process = setTableAliasedNamePipeLine.process(input, new SetTableAliasedName(QualifiedName.of("abc"), new AliasedName("abc")));
        assertEquals(process.getClass(), CreateAdsTable.class);
        assertEquals(process.getAliasedName(), new AliasedName("abc"));
    }
}