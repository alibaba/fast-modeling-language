/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.api.context.setting;

import com.aliyun.fastmodel.core.tree.statement.select.Query;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 转为view的context
 *
 * @author panguanjing
 * @date 2022/5/25
 */
@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ViewSetting {
    /**
     * 是否转为view
     */
    private boolean transformToView;

    /**
     * 是否转为物化视图
     */
    private boolean transformToMaterializedView;

    /**
     * 是否直接使用代码, 那么在转换的手，将以queryCode作为拼接
     */
    private boolean useQueryCode;

    /**
     * 转为的select的对象内容
     */
    private Query query;

    /**
     * 查询的code
     */
    private String queryCode;
}
