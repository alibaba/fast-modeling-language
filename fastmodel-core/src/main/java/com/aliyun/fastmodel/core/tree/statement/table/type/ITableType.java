/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.core.tree.statement.table.type;

/**
 * tableType
 *
 * @author panguanjing
 * @date 2022/6/12
 */
public interface ITableType {
    /**
     * table Type value
     *
     * @return
     */
    String getCode();

    /**
     * description
     *
     * @return 表描述
     */
    String getDescription();
}
