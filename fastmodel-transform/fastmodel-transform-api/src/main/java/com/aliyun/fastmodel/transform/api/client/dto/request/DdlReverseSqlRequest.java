/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.api.client.dto.request;

import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ddl reverse sql request
 *
 * @author panguanjing
 * @date 2022/6/16
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DdlReverseSqlRequest {

    /**
     * database
     */
    private String database;

    /**
     * schema
     */
    private String schema;

    /**
     * ddl code
     */
    private String code;

    /**
     * dialect meta
     */
    private DialectMeta dialectMeta;
}
