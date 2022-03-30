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

package com.aliyun.fastmodel.core.tree.statement.pipe;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * 拷贝管道操作处理
 *
 * @author panguanjing
 * @date 2021/4/6
 */
@Getter
@Setter
@ToString
public class PipeCopyInto {
    private TargetType targetType = TargetType.TABLE;
    private String target;
    private CopyMode copyMode = CopyMode.REPLACE;
}
