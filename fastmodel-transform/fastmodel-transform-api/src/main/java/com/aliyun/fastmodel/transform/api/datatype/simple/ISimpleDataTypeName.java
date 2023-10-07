/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.api.datatype.simple;

import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;

/**
 * 简单数据类型名称
 * @author panguanjing
 * @date 2022/11/7
 */
public interface ISimpleDataTypeName extends IDataTypeName {
    /**
     * 转换为simple dataType
     * @return
     */
    SimpleDataTypeName getSimpleDataTypeName();
}
