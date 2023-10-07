/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.compare.util;

import com.aliyun.fastmodel.core.tree.statement.constants.TableType;
import com.aliyun.fastmodel.core.tree.statement.table.CreateAdsTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDimTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDwsTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateFactTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateOdsTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable.TableBuilder;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2022/10/11
 */
public class MergeTableUtilTest {

    @Test
    public void getSuppier() {
        TableBuilder suppier = MergeTableUtil.getSuppier(TableType.DIM);
        assertEquals(suppier.getClass(), CreateDimTable.builder().getClass());
        suppier = MergeTableUtil.getSuppier(TableType.ODS);
        assertEquals(suppier.getClass(), CreateOdsTable.builder().getClass());
        suppier = MergeTableUtil.getSuppier(TableType.ADS);
        assertEquals(suppier.getClass(), CreateAdsTable.builder().getClass());
        suppier = MergeTableUtil.getSuppier(TableType.FACT);
        assertEquals(suppier.getClass(), CreateFactTable.builder().getClass());
        suppier = MergeTableUtil.getSuppier(TableType.DWS);
        assertEquals(suppier.getClass(), CreateDwsTable.builder().getClass());
    }
}