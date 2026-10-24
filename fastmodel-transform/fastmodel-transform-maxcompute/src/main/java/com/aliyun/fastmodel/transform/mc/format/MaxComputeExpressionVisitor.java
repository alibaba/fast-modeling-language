/*
 * Copyright 2021 Alibaba Group Holding Ltd.
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

package com.aliyun.fastmodel.transform.mc.format;

import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.literal.CurrentDate;
import com.aliyun.fastmodel.core.tree.expr.literal.CurrentTimestamp;
import com.aliyun.fastmodel.transform.api.format.DefaultExpressionVisitor;
import com.aliyun.fastmodel.transform.mc.parser.util.MaxComputeReservedWordUtil;

/**
 * MaxComputeExpressionVisitor
 *
 * @author panguanjing
 * @date 2021/4/13
 */
public class MaxComputeExpressionVisitor extends DefaultExpressionVisitor {

    @Override
    public String visitCurrentTimestamp(CurrentTimestamp currentTimestamp, Void context) {
        return "GETDATE()";
    }

    @Override
    public String visitCurrentDate(CurrentDate currentDate, Void context) {
        return "GETDATE()";
    }


}
