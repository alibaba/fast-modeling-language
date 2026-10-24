package com.aliyun.fastmodel.transform.mc.parser.visitor;

import java.util.List;

import com.aliyun.fastmodel.common.parser.ParserHelper;
import com.aliyun.fastmodel.common.utils.StripUtils;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.atom.FunctionCall;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.transform.mc.parser.OdpsParser.BooleanValueContext;
import com.aliyun.fastmodel.transform.mc.parser.OdpsParser.CharSetStringLiteralContext;
import com.aliyun.fastmodel.transform.mc.parser.OdpsParser.ConstantContext;
import com.aliyun.fastmodel.transform.mc.parser.OdpsParser.FunctionContext;
import com.aliyun.fastmodel.transform.mc.parser.OdpsParser.FunctionNameContext;
import com.aliyun.fastmodel.transform.mc.parser.OdpsParser.IdentifierContext;
import com.aliyun.fastmodel.transform.mc.parser.OdpsParser.StringLiteralContext;
import com.aliyun.fastmodel.transform.mc.parser.OdpsParserBaseVisitor;

import static com.aliyun.fastmodel.common.parser.ParserHelper.getLocation;
import static com.aliyun.fastmodel.common.parser.ParserHelper.getOrigin;

/**
 * expression output visitor
 *
 * @author panguanjing
 * @date 2022/5/18
 */
public class MaxComputeExpressionAstBuilder extends OdpsParserBaseVisitor<Node> {

    @Override
    public Node visitStringLiteral(StringLiteralContext ctx) {
        return new StringLiteral(StripUtils.strip(ctx.getText()));
    }

    @Override
    public Node visitIdentifier(IdentifierContext ctx) {
        return ParserHelper.getIdentifier(ctx);
    }

    @Override
    public Node visitFunction(FunctionContext ctx) {
        FunctionNameContext functionNameContext = ctx.functionName();
        List<BaseExpression> list = ParserHelper.visit(this, ctx.functionArgument(), BaseExpression.class);
        QualifiedName qualifiedName = QualifiedName.of(functionNameContext.getText());
        return new FunctionCall(
            getLocation(ctx),
            getOrigin(ctx),
            qualifiedName,
            ctx.distinct != null,
            list,
            null,
            null,
            null,
            null
        );
    }

    @Override
    public Node visitConstant(ConstantContext ctx) {
        BooleanValueContext booleanValueContext = ctx.booleanValue();
        if (booleanValueContext != null) {
            return visit(booleanValueContext);
        }
        CharSetStringLiteralContext charSetStringLiteralContext = ctx.charSetStringLiteral();
        if (charSetStringLiteralContext != null) {
            return visit(charSetStringLiteralContext);
        }
        if (ctx.timestampLiteral() != null) {
            return visit(ctx.timestampLiteral());
        }
        if (ctx.dateLiteral() != null) {
            return visit(ctx.dateLiteral());
        }
        if (ctx.intervalLiteral() != null) {
            return visit(ctx.intervalLiteral());
        }
        if (ctx.stringLiteral() != null) {
            return visit(ctx.stringLiteral());
        }
        if (ctx.bi != null) {
            return new LongLiteral(getLocation(ctx), getOrigin(ctx), ctx.bi.getText());
        }
        if (ctx.si != null) {
            return new LongLiteral(getLocation(ctx), getOrigin(ctx), ctx.si.getText());
        }
        return super.visitConstant(ctx);
    }

}
