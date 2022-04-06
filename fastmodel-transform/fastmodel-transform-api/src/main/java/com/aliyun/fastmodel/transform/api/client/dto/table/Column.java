/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.api.client.dto.table;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 列的client DTO对象
 *
 * @author panguanjing
 * @date 2022/6/6
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Column {
    /**
     * id, if first create set to null,
     * else set to name
     */
    private String id;
    /**
     * name, column name
     */
    private String name;
    /**
     * data type name
     */
    private String dataType;
    /**
     * length，长度
     */
    private Integer length;
    /**
     * comment
     */
    private String comment;
    /**
     * precision
     * 精度
     */
    private Integer precision;
    /**
     * scale
     */
    private Integer scale;
    /**
     * primary key
     */
    private boolean primaryKey;
    /**
     * nullable
     */
    private boolean nullable;
    /**
     * partitionKey key
     */
    private boolean partitionKey;
    /**
     * partition key index, zero-base
     */
    private Integer partitionKeyIndex;
}
