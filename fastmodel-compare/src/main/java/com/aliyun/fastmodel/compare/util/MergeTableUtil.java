/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.compare.util;

import java.util.Map;
import java.util.function.Supplier;

import com.aliyun.fastmodel.core.tree.statement.constants.TableType;
import com.aliyun.fastmodel.core.tree.statement.table.CreateAdsTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateCodeTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDimTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDwsTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateFactTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable.TableBuilder;
import com.aliyun.fastmodel.core.tree.statement.table.type.ITableType;
import com.google.common.collect.Maps;

/**
 * merge Table helper
 *
 * @author panguanjing
 * @date 2022/10/11
 */
public class MergeTableUtil {

    private static Map<ITableType, Supplier<TableBuilder>> builderMap = Maps.newHashMap();

    static {
        init();
    }

    private static void init() {
        builderMap.put(TableType.ADS, () -> CreateAdsTable.builder());
        builderMap.put(TableType.DWS, () -> CreateDwsTable.builder());
        builderMap.put(TableType.DIM, () -> CreateDimTable.builder());
        builderMap.put(TableType.FACT, () -> CreateFactTable.builder());
        builderMap.put(TableType.CODE, () -> CreateCodeTable.builder());
    }

    public static TableBuilder getSuppier(ITableType tableType) {
        Supplier<TableBuilder> tableBuilderSupplier = builderMap.get(tableType);
        if (tableBuilderSupplier == null) {
            return CreateTable.builder();
        }
        return tableBuilderSupplier.get();
    }
}
