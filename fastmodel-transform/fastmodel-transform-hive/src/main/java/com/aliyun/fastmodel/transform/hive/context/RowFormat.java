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

package com.aliyun.fastmodel.transform.hive.context;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/2/2
 */
@Getter
@Setter
@AllArgsConstructor
public class RowFormat {
    private String fieldTerminated;
    private String escaped;
    private String collectionTerminated;
    private String mapTerminated;
    private String lineTerminated;
    private String nullDefined;
}
