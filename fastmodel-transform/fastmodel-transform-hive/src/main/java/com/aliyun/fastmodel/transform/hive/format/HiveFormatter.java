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

package com.aliyun.fastmodel.transform.hive.format;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.hive.context.HiveTransformContext;
import org.apache.commons.lang3.BooleanUtils;

import static com.aliyun.fastmodel.transform.api.context.TransformContext.SEMICOLON;

/**
 * 格式化处理操作内容
 *
 * @author panguanjing
 * @date 2021/2/1
 */
public final class HiveFormatter {

    public static DialectNode format(BaseStatement source, HiveTransformContext context) {
        HiveVisitor hiveVisitor = new HiveVisitor(context);
        Boolean process = hiveVisitor.process(source, 0);
        Boolean append = context.isAppendSemicolon();
        String result = hiveVisitor.getBuilder().toString();
        if (BooleanUtils.isTrue(append)) {
            result = result + SEMICOLON;
        }
        return new DialectNode(result, process);
    }
}
