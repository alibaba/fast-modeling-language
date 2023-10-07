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

package com.aliyun.fastmodel.core.tree.util;

import java.util.List;

import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.ComparisonExpression;
import com.aliyun.fastmodel.core.tree.expr.LogicalBinaryExpression;
import com.aliyun.fastmodel.core.tree.expr.atom.TableOrColumn;
import com.aliyun.fastmodel.core.tree.expr.enums.ComparisonOperator;
import com.aliyun.fastmodel.core.tree.expr.enums.LogicalOperator;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import org.junit.Test;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/3/18
 */
public class ExpressionUtilsTest {

    @Test
    public void testLogic() {
        LogicalBinaryExpression logicalBinaryExpression =
            new LogicalBinaryExpression(
                LogicalOperator.AND,
                new ComparisonExpression(
                    ComparisonOperator.EQUAL,
                    new TableOrColumn(QualifiedName.of("code")),
                    new StringLiteral("code")
                ),
                new ComparisonExpression(
                    ComparisonOperator.GREATER_THAN,
                    new TableOrColumn(QualifiedName.of("name")),
                    new LongLiteral("1")
                )
            );
        List<BaseExpression> baseExpressions = ExpressionUtil.extractPredicates(logicalBinaryExpression);
        System.out.println(baseExpressions);
    }
}
