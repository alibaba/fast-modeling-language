/*
 * Copyright 2020 Alibaba Group Holding Ltd.
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

/*
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
package com.aliyun.fastmodel.transform.mc.format;

import com.aliyun.fastmodel.core.tree.AbstractNode;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.mc.context.MaxComputeContext;
import org.apache.commons.lang3.StringUtils;

import static com.aliyun.fastmodel.transform.api.context.TransformContext.SEMICOLON;

/**
 * sql的format处理
 *
 * @author panguanjing
 */
public final class MaxComputeFormatter {

    private MaxComputeFormatter() {}

    public static DialectNode formatSql(AbstractNode root, MaxComputeContext transformContext) {
        MaxComputeVisitor formatter = new MaxComputeVisitor(transformContext);
        Boolean process = formatter.process(root, 0);
        String result = formatter.getBuilder().toString();
        if (StringUtils.isBlank(result)) {
            return new DialectNode(result, process);
        }
        if (transformContext.isAppendSemicolon() && !StringUtils.endsWith(result, SEMICOLON)) {
            result = result + SEMICOLON;
        }
        return new DialectNode(result, process);
    }

}
