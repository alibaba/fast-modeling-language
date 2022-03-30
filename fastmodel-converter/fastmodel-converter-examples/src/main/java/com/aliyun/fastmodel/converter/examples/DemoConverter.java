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

package com.aliyun.fastmodel.converter.examples;

import com.aliyun.fastmodel.converter.spi.ConvertContext;
import com.aliyun.fastmodel.conveter.dqc.BaseDqcStatementConverter;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.rule.DropRule;
import com.aliyun.fastmodel.core.tree.statement.rule.RulesLevel;
import com.aliyun.fastmodel.core.tree.statement.table.DropConstraint;

/**
 * demo converter
 *
 * @author panguanjing
 * @date 2021/6/14
 */
public class DemoConverter extends BaseDqcStatementConverter<DropConstraint> {
    @Override
    public BaseStatement convert(DropConstraint source, ConvertContext context) {
        return new DropRule(RulesLevel.SQL, QualifiedName.of("abc"), new Identifier("col1"));
    }
}
