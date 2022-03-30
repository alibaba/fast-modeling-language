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
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.exception.ParseException;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.script.ImportObject;
import com.aliyun.fastmodel.core.tree.statement.script.RefDirection;
import com.aliyun.fastmodel.core.tree.statement.script.RefObject;
import com.aliyun.fastmodel.core.tree.statement.script.RefRelation;
import com.aliyun.fastmodel.core.tree.util.IdentifierUtil;
import com.aliyun.fastmodel.parser.AstBuilder;
import com.aliyun.fastmodel.parser.annotation.SubVisitor;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.CommentContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.IdentifierContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.ImportEntityStatementContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.RefEntityStatementContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.TableNameListContext;
import com.google.common.collect.Lists;

/**
 * ScriptVisitor
 *
 * @author panguanjing
 * @date 2021/9/14
 */
@SubVisitor
public class ScriptVisitor extends AstBuilder {

    @Override
    public Node visitImportEntityStatement(ImportEntityStatementContext ctx) {
        return new ImportObject(getQualifiedName(ctx.qualifiedName()),
            visitIfPresent(ctx.identifier(), Identifier.class).orElse(null));
    }

    @Override
    public Node visitRefEntityStatement(RefEntityStatementContext ctx) {
        IdentifierContext identifier = ctx.identifier();
        Identifier identifier1 = null;
        if (identifier == null) {
            identifier1 = IdentifierUtil.sysIdentifier();
        } else {
            identifier1 = (Identifier)visit(identifier);
        }
        return new RefRelation(QualifiedName.of(Lists.newArrayList(identifier1)), getLeftEntity(ctx),
            getRightEntity(ctx), RefDirection.getByCode(ctx.refRelationType().getText()));
    }

    private RefObject getRightEntity(RefEntityStatementContext ctx) {
        TableNameListContext rightTable = ctx.rightTable;
        return getRefEntity(rightTable, ctx.leftTableComment);
    }

    private RefObject getRefEntity(TableNameListContext rightTable, CommentContext commentContext) {
        List<QualifiedName> qualifiedName = getQualifiedName(rightTable);
        long count = qualifiedName.stream().map(QualifiedName::getFirst).distinct().count();
        if (count > 1) {
            throw new ParseException("only support one table element connect another");
        }
        QualifiedName t = QualifiedName.of(qualifiedName.get(0).getFirst());
        List<Identifier> list = qualifiedName.stream().filter(x -> x.getParts().size() > 1).map(x -> {
            return new Identifier(x.getSuffix());
        }).collect(Collectors.toList());
        Comment c = visitIfPresent(commentContext, Comment.class).orElse(null);
        return new RefObject(t, list, c);
    }

    private RefObject getLeftEntity(RefEntityStatementContext ctx) {
        return getRefEntity(ctx.leftTable, ctx.leftTableComment);
    }
}
