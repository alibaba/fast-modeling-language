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

package com.aliyun.fastmodel.transform.fml.format;

import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.fml.context.FmlTransformContext;

/**
 * FastModelFormatter
 *
 * @author panguanjing
 * @date 2020/11/23
 */
public class FmlFormatter {

    private FmlFormatter() {

    }

    /**
     * format信息
     */
    public static DialectNode formatNode(Node root, FmlTransformContext context) {
        if (root == null) {
            throw new IllegalArgumentException("root can't be null");
        }
        FmlVisitor fmlVisitor = new FmlVisitor(context);
        Boolean process = fmlVisitor.process(root, 0);
        String result = fmlVisitor.getBuilder().toString();
        if (context.isAppendSemicolon() && !result.endsWith(TransformContext.SEMICOLON)) {
            result = result + TransformContext.SEMICOLON;
        }
        return new DialectNode(result, process);
    }

}
