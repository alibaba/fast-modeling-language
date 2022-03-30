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

package com.aliyun.fastmodel.compare;

import java.util.Optional;

import com.aliyun.fastmodel.core.tree.Node;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * 比对的内容对象
 *
 * @author panguanjing
 * @date 2021/11/3
 */
@Getter
@ToString
@EqualsAndHashCode
public class ComparePair {
    private final Optional<Node> left;
    private final Optional<Node> right;

    public ComparePair(Optional<Node> left, Optional<Node> right) {
        this.left = left;
        this.right = right;
    }
}
