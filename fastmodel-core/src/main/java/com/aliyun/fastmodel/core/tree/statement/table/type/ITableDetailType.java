/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.core.tree.statement.table.type;

/**
 * ITableDetailType
 *
 * @author panguanjing
 * @date 2022/7/10
 */
public interface ITableDetailType extends ITableType {

    /**
     * get parent Table Type
     *
     * @return {@link ITableType}
     */
    ITableType getParent();

    /**
     * is single detail Type
     *
     * @return
     */
    default boolean isSingle() {
        return getCode().equalsIgnoreCase(getParent().getCode());
    }

}
