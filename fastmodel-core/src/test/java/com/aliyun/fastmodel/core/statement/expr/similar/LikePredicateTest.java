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

package com.aliyun.fastmodel.core.statement.expr.similar;

import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.atom.TableOrColumn;
import com.aliyun.fastmodel.core.tree.expr.enums.LikeCondition;
import com.aliyun.fastmodel.core.tree.expr.enums.LikeOperator;
import com.aliyun.fastmodel.core.tree.expr.similar.LikePredicate;
import org.hamcrest.Matchers;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;

/**
 * like expr
 *
 * @author panguanjing
 * @date 2020/10/23
 */
public class LikePredicateTest {

    @Test
    public void testToString() {
        Identifier abc = new Identifier("abc");
        TableOrColumn abc1 = new TableOrColumn(QualifiedName.of("abc"));
        LikePredicate expr = new LikePredicate(abc, LikeCondition.ANY, LikeOperator.LIKE, abc1);
        expr.setParenthesized(true);
        assertThat("(abc LIKE ANY abc)", Matchers.equalToIgnoringCase(expr.toString()));
    }
}