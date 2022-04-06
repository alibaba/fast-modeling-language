/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.api.context.setting;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * query convert context
 *
 * @author panguanjing
 * @date 2022/7/2
 */
@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class QuerySetting {
    /**
     * 转换时是否仍然保留schema，默认是false，不保留
     */
    private boolean keepSchemaName;

}
