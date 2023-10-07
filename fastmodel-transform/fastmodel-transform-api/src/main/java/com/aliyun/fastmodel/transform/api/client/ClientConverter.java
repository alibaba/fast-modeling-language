/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.api.client;

import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.client.dto.table.TableConfig;
import com.aliyun.fastmodel.transform.api.context.TransformContext;

/**
 * ClientConverter
 *
 * @author panguanjing
 * @date 2022/8/5
 */
public interface ClientConverter<T extends TransformContext> {
    /**
     * convert to node
     *
     * @param table
     * @return {@link  Node}
     */
    Node covertToNode(Table table, TableConfig tableConfig);

    /**
     * convert to table
     *
     * @param table
     * @param context
     * @return {@link Table}
     */
    Table convertToTable(Node table, T context);
}
