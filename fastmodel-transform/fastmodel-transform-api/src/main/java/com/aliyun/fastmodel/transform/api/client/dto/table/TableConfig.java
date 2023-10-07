/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.api.client.dto.table;

import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * table client config
 *
 * @author panguanjing
 * @date 2022/6/6
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TableConfig {

    /**
     * 方言名
     */
    private DialectMeta dialectMeta;
    /**
     * 默认忽略大小写
     */
    private boolean caseSensitive;

    /**
     * 是否先删除原有的表
     * 默认不生成
     */
    private boolean dropIfExist;

    /**
     * 生成的sql是否需要增加分号
     */
    @Default
    private boolean appendSemicolon = true;

}
