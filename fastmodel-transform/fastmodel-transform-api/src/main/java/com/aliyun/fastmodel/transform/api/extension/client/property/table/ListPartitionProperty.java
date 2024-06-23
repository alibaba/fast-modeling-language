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

package com.aliyun.fastmodel.transform.api.extension.client.property.table;

import com.alibaba.fastjson.JSON;

import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.partition.ListClientPartition;
import org.apache.commons.lang3.StringUtils;

import static com.aliyun.fastmodel.transform.api.extension.client.property.ExtensionPropertyKey.TABLE_LIST_PARTITION;

/**
 * @author 子梁
 * @date 2023/12/25
 */
public class ListPartitionProperty extends BaseClientProperty<ListClientPartition> {

    public ListPartitionProperty() {
        this.setKey(TABLE_LIST_PARTITION.getValue());
    }

    @Override
    public String valueString() {
        return JSON.toJSONString(value);
    }

    @Override
    public void setValueString(String value) {
        if (StringUtils.isBlank(value)) {
            return;
        }
        this.setValue(JSON.parseObject(value, ListClientPartition.class));
    }
}
