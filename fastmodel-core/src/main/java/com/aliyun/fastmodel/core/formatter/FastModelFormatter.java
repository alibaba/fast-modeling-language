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

package com.aliyun.fastmodel.core.formatter;

import com.aliyun.fastmodel.core.tree.Node;

/**
 * FastModelFormatter
 *
 * @author panguanjing
 * @date 2020/11/23
 */
public class FastModelFormatter {

    public static String formatNode(Node root) {
        if (root == null) {
            throw new IllegalArgumentException("root can't be null");
        }
        FastModelVisitor fastModelVisitor = new FastModelVisitor();
        fastModelVisitor.process(root, 0);
        return fastModelVisitor.getBuilder().toString();
    }

}
