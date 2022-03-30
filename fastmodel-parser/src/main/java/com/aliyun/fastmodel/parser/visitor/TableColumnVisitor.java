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

import com.aliyun.fastmodel.core.tree.AliasedName;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.constants.ColumnCategory;
import com.aliyun.fastmodel.core.tree.statement.table.AddCols;
import com.aliyun.fastmodel.core.tree.statement.table.AddPartitionCol;
import com.aliyun.fastmodel.core.tree.statement.table.ChangeCol;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.DropPartitionCol;
import com.aliyun.fastmodel.core.tree.statement.table.RenameCol;
import com.aliyun.fastmodel.core.tree.statement.table.SetColComment;
import com.aliyun.fastmodel.core.tree.statement.table.SetColProperties;
import com.aliyun.fastmodel.core.tree.statement.table.SetColumnOrder;
import com.aliyun.fastmodel.core.tree.statement.table.UnSetColProperties;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.parser.AstBuilder;
import com.aliyun.fastmodel.parser.annotation.SubVisitor;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.AddColContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.AddPartitionColContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.AlterColumnConstraintContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.AlterStatementSuffixRenameColContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.CategoryContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.ChangeAllContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.CommentContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.DropPartitionColContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.ReferenceColumnListContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.ReferencesObjectsContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.RenameColContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SetColCommentContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SetColPropertiesContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SetColumnOrderContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.UnSetColPropertiesContext;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.BooleanUtils;

import static com.aliyun.fastmodel.common.parser.ParserHelper.getLocation;
import static com.aliyun.fastmodel.common.parser.ParserHelper.getOrigin;
import static java.util.stream.Collectors.toList;

/**
 * TableColumnVisitor
 *
 * @author panguanjing
 * @date 2021/4/9
 */
@SubVisitor
public class TableColumnVisitor extends AstBuilder {

    @Override
    public Node visitAddCol(AddColContext ctx) {
        List<ColumnDefinition> columnDefines = visit(
            ctx.alterStatementSuffixAddCol().columnNameTypeList().columnNameType(),
            ColumnDefinition.class);
        return new AddCols(getQualifiedName(ctx.tableName()), columnDefines);
    }

    @Override
    public Node visitChangeAll(ChangeAllContext ctx) {
        AlterStatementSuffixRenameColContext renameColContext = ctx.alterStatementSuffixRenameCol();
        AlterColumnConstraintContext tree = renameColContext.alterColumnConstraint();
        BaseConstraint baseConstraint = visitIfPresent(tree, BaseConstraint.class).orElse(null);
        Boolean primary = isPrimary(baseConstraint);
        Boolean notNull = isNotNull(baseConstraint);
        if (BooleanUtils.isTrue(primary)) {
            notNull = true;
        }
        CommentContext comment = renameColContext.comment();
        AliasedName aliasedName = visitIfPresent(ctx.alterStatementSuffixRenameCol().alias(), AliasedName.class).orElse(
            null);
        Identifier visit = (Identifier)visit(renameColContext.newIdentifier);
        Comment visit1 = null;
        if (comment != null) {
            visit1 = (Comment)visit(comment);
        }
        BaseDataType baseDataType = (BaseDataType)visit(renameColContext.colType());
        List<Property> properties = ImmutableList.of();
        if (renameColContext.setProperties() != null) {
            properties = visit(renameColContext.setProperties().tableProperties().keyValueProperty(),
                Property.class);
        }
        CategoryContext category = renameColContext.category();
        ColumnCategory columnCategory = null;
        if (category != null) {
            columnCategory = ColumnCategory.getByCode(category.getText());
        }
        List<Identifier> indicators = ImmutableList.of();
        QualifiedName refDimensions = null;
        ReferencesObjectsContext referencesObjectsContext = renameColContext.referencesObjects();
        if (referencesObjectsContext != null) {
            ReferenceColumnListContext referenceColumnListContext = referencesObjectsContext.referenceColumnList();
            if (referenceColumnListContext != null) {
                indicators =
                    visit(referenceColumnListContext.columnParenthesesList().columnNameList().columnName(),
                        Identifier.class);
            } else {
                refDimensions = getQualifiedName(referencesObjectsContext.qualifiedName());
            }
        }
        return new ChangeCol(getQualifiedName(ctx.tableName()),
            (Identifier)visit(renameColContext.old),
            ColumnDefinition.builder()
                .colName(visit)
                .aliasedName(aliasedName)
                .primary(primary)
                .notNull(notNull)
                .category(columnCategory)
                .comment(visit1)
                .properties(properties)
                .dataType(baseDataType)
                .refIndicators(indicators)
                .refDimension(refDimensions)
                .build()
        );
    }

    @Override
    public Node visitRenameCol(RenameColContext ctx) {
        return new RenameCol(
            getQualifiedName(ctx.tableName()),
            (Identifier)visit(ctx.left),
            (Identifier)visit(ctx.right)
        );
    }

    @Override
    public Node visitSetColComment(SetColCommentContext ctx) {
        StringLiteral stringLiteral = (StringLiteral)visit(ctx.string());
        return new SetColComment(
            getQualifiedName(ctx.tableName()),
            (Identifier)visit(ctx.identifier()),
            new Comment(stringLiteral.getValue())
        );
    }

    @Override
    public Node visitSetColProperties(SetColPropertiesContext ctx) {
        return new SetColProperties(
            getQualifiedName(ctx.tableName()),
            (Identifier)visit(ctx.identifier()),
            visit(ctx.setProperties().tableProperties().keyValueProperty(), Property.class)
        );
    }

    @Override
    public Node visitUnSetColProperties(UnSetColPropertiesContext ctx) {
        List<Identifier> identifiers = getUnSetProperties(ctx.unSetProperties());
        return new UnSetColProperties(
            getQualifiedName(ctx.tableName()),
            (Identifier)visit(ctx.identifier()),
            identifiers.stream().map(Identifier::getValue).collect(toList())
        );
    }

    @Override
    public Node visitAddPartitionCol(AddPartitionColContext ctx) {
        ColumnDefinition visit = (ColumnDefinition)visit(ctx.columnDefinition());
        return new AddPartitionCol(
            getLocation(ctx),
            getOrigin(ctx),
            getQualifiedName(ctx.tableName()),
            visit
        );
    }

    @Override
    public Node visitDropPartitionCol(DropPartitionColContext ctx) {
        return new DropPartitionCol(
            getQualifiedName(ctx.tableName()),
            (Identifier)visit(ctx.columnName())
        );
    }

    @Override
    public Node visitSetColumnOrder(SetColumnOrderContext ctx) {
        return new SetColumnOrder(
            getQualifiedName(ctx.tableName()),
            getIdentifier(ctx.left),
            getIdentifier(ctx.right),
            (BaseDataType)visit(ctx.colType()),
            getIdentifier(ctx.bf),
            ctx.KW_FIRST() != null);
    }
}
