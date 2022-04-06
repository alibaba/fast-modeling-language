/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.core.tree;

/**
 * istatement type
 *
 * @author panguanjing
 * @date 2022/6/9
 */
public interface IStatementType {

    /**
     * @return
     */
    String getCode();

    /**
     * 简写默认于code一致
     *
     * @return
     */
    default String getShortCode() {
        return getCode();
    }
}
