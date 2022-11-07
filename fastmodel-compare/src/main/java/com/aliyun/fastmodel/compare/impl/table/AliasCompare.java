/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.compare.impl.table;

import java.util.List;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableAliasedName;
import com.google.common.collect.ImmutableList;

/**
 * AliasCompare
 *
 * @author panguanjing
 * @date 2021/8/30
 */
public class AliasCompare implements TableElementCompare {
    @Override
    public List<BaseStatement> compareTableElement(CreateTable before, CreateTable after) {
        boolean change = before.getAliasedName() == null || !before.getAliasedName().equals(after.getAliasedName());
        boolean afterCommentNotNull = after.getAliasedName() != null;
        if (change && afterCommentNotNull) {
            return ImmutableList.of(new SetTableAliasedName(after.getQualifiedName(), after.getAliasedName()));
        }
        return ImmutableList.of();
    }

}
