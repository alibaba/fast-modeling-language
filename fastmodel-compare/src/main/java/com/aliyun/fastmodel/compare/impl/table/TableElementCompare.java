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

/**
 * 比对表元素信息
 *
 * @author panguanjing
 * @date 2021/8/30
 */
public interface TableElementCompare {
    /**
     * 比对
     *
     * @param before
     * @param after
     * @return
     */
    List<BaseStatement> compareTableElement(CreateTable before, CreateTable after);
}
