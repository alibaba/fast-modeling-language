/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.compare.merge;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2022/10/9
 */
public interface Pipeline<T extends BaseStatement, R extends BaseStatement> {
    R process(R input, T baseStatement);
}
