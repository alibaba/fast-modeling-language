/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.api.client.dto.result;

import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ddl table result
 *
 * @author panguanjing
 * @date 2022/6/16
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class DdlTableResult {

    /**
     * result table
     */
    private Table table;

}
