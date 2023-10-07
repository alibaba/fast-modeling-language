/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.api.client;

import com.aliyun.fastmodel.transform.api.client.dto.request.DdlGeneratorModelRequest;
import com.aliyun.fastmodel.transform.api.client.dto.request.DdlGeneratorSqlRequest;
import com.aliyun.fastmodel.transform.api.client.dto.request.DdlReverseSqlRequest;
import com.aliyun.fastmodel.transform.api.client.dto.result.DdlGeneratorResult;
import com.aliyun.fastmodel.transform.api.client.dto.result.DdlTableResult;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;

/**
 * 客户端的请求
 *
 * @author panguanjing
 * @date 2022/6/6
 */
public interface CodeGenerator {

    /**
     * 代码生成器
     * 支持传入新的ddl进行对比
     * 返回ddl的语句内容
     *
     * @param request 请求参数
     * @return {@link DdlGeneratorResult}
     */
    DdlGeneratorResult generate(DdlGeneratorModelRequest request);

    /**
     * 代码生成器
     * 支持传入新的ddl进行对比
     * 返回ddl的语句内容
     *
     * @param request 请求参数
     * @return {@link DdlGeneratorResult}
     */
    DdlGeneratorResult generate(DdlGeneratorSqlRequest request);

    /**
     * 将ddl转为Table Result
     *
     * @param request 请求参数
     * @return {@link DdlTableResult}
     */
    DdlTableResult reverse(DdlReverseSqlRequest request);

}
