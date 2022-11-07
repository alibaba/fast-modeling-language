/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.api.client.dto.result;

import java.util.List;

import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ddl Generator Result
 *
 * @author panguanjing
 * @date 2022/6/7
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DdlGeneratorResult {
    /**
     * 生成方言的node内容
     */
    private List<DialectNode> dialectNodes;

}
