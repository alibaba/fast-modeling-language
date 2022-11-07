/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.api.builder.merge;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

/**
 * 合并结果对象
 *
 * @author panguanjing
 * @date 2022/6/29
 */
@Getter
@Setter
@AllArgsConstructor
public class MergeResult {
    /**
     * 合并后的语句
     */
    private BaseStatement statement;
    /**
     * 是否合并成功
     */
    private boolean mergeSuccess;
}
