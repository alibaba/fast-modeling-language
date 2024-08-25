/*
 * Copyright [2024] [name of copyright owner]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.fastmodel.transform.hologres.client.property;

import java.util.Collections;
import java.util.List;

import com.aliyun.fastmodel.common.utils.StripUtils;
import com.aliyun.fastmodel.core.tree.statement.select.order.Ordering;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

/**
 * ColumnOrder
 *
 * @author panguanjing
 * @date 2024/4/19
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ColumnOrder {
    /**
     * 列名
     */
    private String columnName;

    /**
     * 排序方式
     */
    private Ordering order;

    /**
     * column:order, column
     *
     * @param value
     * @return {@see ColumnOrder}
     */
    public static List<ColumnOrder> of(String value) {
        if (StringUtils.isBlank(value)) {
            return Collections.emptyList();
        }
        String val = StripUtils.removeDoubleStrip(value);
        List<String> list = Splitter.on(",").splitToList(val);
        List<ColumnOrder> columnOrders = Lists.newArrayList();
        for (String s : list) {
            List<String> splitToList = Splitter.on(":").splitToList(s);
            ColumnOrder columnStatus = null;
            if (splitToList.size() > 1) {
                columnStatus = new ColumnOrder(
                    splitToList.get(0).trim(),
                    Ordering.getByCode(splitToList.get(1).trim())
                );
            } else {
                columnStatus = new ColumnOrder(
                    splitToList.get(0).trim(),
                    null
                );
            }
            columnOrders.add(columnStatus);
        }
        return columnOrders;
    }
}
