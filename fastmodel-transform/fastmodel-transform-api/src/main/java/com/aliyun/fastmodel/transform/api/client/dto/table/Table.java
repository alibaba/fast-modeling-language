/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.api.client.dto.table;

import java.util.List;

import com.aliyun.fastmodel.transform.api.client.dto.constraint.Constraint;
import com.aliyun.fastmodel.transform.api.client.dto.index.Index;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * table client
 *
 * @author panguanjing
 * @date 2022/6/6
 * @date 2024/2/19 add the index
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Table {

    /**
     * 是否外部表
     */
    private boolean external;

    /**
     * catalog名称
     */
    private String catalog;

    /**
     * database 名字
     */
    private String database;

    /**
     * schema名称
     */
    private String schema;

    /**
     * 表名
     */
    private String name;

    /**
     * 备注
     */
    private String comment;

    /**
     * 默认是true
     */
    @Default
    private boolean ifNotExist = true;

    /**
     * 列
     */
    private List<Column> columns;

    /**
     * 约束的clientDTO
     */
    private List<Constraint> constraints;

    /**
     * 索引列表
     */
    private List<Index> indices;

    /**
     * 生命周期, 单位秒
     */
    private Long lifecycleSeconds;

    /**
     * table properties
     */
    private List<BaseClientProperty> properties;

}
