/*
 * Copyright 2021-2022 Alibaba Group Holding Ltd.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.aliyun.fastmodel.driver.model;

import java.io.Serializable;
import java.util.List;

import lombok.Getter;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/12/9
 */
@Getter
public class DriverRow implements Serializable {
    private static final long serialVersionUID = 40997225905240661L;

    private final List<?> data;

    public DriverRow(List<?> data) {
        this.data = data;
    }

    /**
     * 根据索引获取数据
     *
     * @param index
     * @return
     */
    public Object getValue(Integer index) {
        return data.get(index);
    }
}
