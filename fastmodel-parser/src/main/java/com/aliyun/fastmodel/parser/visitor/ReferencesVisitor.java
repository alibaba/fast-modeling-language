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

package com.aliyun.fastmodel.parser.visitor;

import java.util.List;

import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.constants.ShowType;
import com.aliyun.fastmodel.core.tree.statement.references.MoveReferences;
import com.aliyun.fastmodel.core.tree.statement.references.ShowReferences;
import com.aliyun.fastmodel.parser.AstBuilder;
import com.aliyun.fastmodel.parser.annotation.SubVisitor;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.MoveReferencesContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.ShowReferencesContext;

/**
 * references visitor
 *
 * @author panguanjing
 * @date 2022/2/18
 */
@SubVisitor
public class ReferencesVisitor extends AstBuilder {

    @Override
    public Node visitShowReferences(ShowReferencesContext ctx) {
        Identifier visit = (Identifier)visit(ctx.showType());
        return new ShowReferences(
            ShowType.getByCode(visit.getValue()),
            getQualifiedName(ctx.qualifiedName()),
            getProperties(ctx.setProperties())
        );
    }

    @Override
    public Node visitMoveReferences(MoveReferencesContext ctx) {
        ShowType supportObjectType = ShowType.getByCode(ctx.showType().getText());
        List<Property> list = getProperties(ctx.setProperties());
        return new MoveReferences(
            supportObjectType,
            getQualifiedName(ctx.from),
            getQualifiedName(ctx.to),
            list
        );
    }
}
