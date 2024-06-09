/*
 * Copyright (c)  2020. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.common.parser;

import java.math.BigDecimal;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.literal.BaseLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.DecimalLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.DoubleLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.AbstractParseTreeVisitor;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.lang3.StringUtils;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/11/19
 */
public class ParserHelper {

    public static final String REGEX = "`";
    public static final String DQUOTA = "\"";

    /**
     * 获取原始的文本
     *
     * @param parserRuleContext context
     * @return 原始的内容
     */
    public static String getOrigin(ParserRuleContext parserRuleContext) {
        Token start = parserRuleContext.getStart();
        int startIndex = start.getStartIndex();
        Token stop = parserRuleContext.getStop();
        int endIndex = stop.getStopIndex();
        Interval interval = new Interval(startIndex, endIndex);
        CharStream input = start.getInputStream();
        if (input == null) {
            return null;
        }
        return input.getText(interval);
    }

    public static String getOrigin(Token token) {
        int startIndex = token.getStartIndex();
        int endIndex = token.getStopIndex();
        Interval interval = new Interval(startIndex, endIndex);
        CharStream input = token.getInputStream();
        return input.getText(interval);
    }

    public static String getOrigin(TerminalNode token) {
        return getOrigin(token.getSymbol());
    }

    /**
     * 根据baseLiteral，获取数字，如果不是指定几种数字类型的字变量，那么返回false
     *
     * @param baseLiteral
     * @return
     */
    public static Number getNumber(BaseLiteral baseLiteral) {
        Number number = null;
        if (baseLiteral instanceof LongLiteral) {
            number = ((LongLiteral)baseLiteral).getValue();
        }
        if (baseLiteral instanceof DoubleLiteral) {
            number = ((DoubleLiteral)baseLiteral).getValue();
        }
        if (baseLiteral instanceof DecimalLiteral) {
            String n = ((DecimalLiteral)baseLiteral).getNumber();
            number = new BigDecimal(n);
        }
        if (baseLiteral instanceof StringLiteral) {
            String n = ((StringLiteral)baseLiteral).getValue();
            number = new BigDecimal(n);
        }
        return number;
    }

    /**
     * 获取节点位置
     *
     * @param parserRuleContext Context
     * @return {@link NodeLocation}
     */
    public static NodeLocation getLocation(ParserRuleContext parserRuleContext) {
        requireNonNull(parserRuleContext, "parserRuleContext is null");
        return getLocation(parserRuleContext.getStart());
    }

    public static NodeLocation getLocation(Token token) {
        requireNonNull(token, "token is null");
        return new NodeLocation(token.getLine(), token.getCharPositionInLine() + 1);
    }

    public static NodeLocation getLocation(TerminalNode terminalNode) {
        requireNonNull(terminalNode, "terminalNode is null");
        return getLocation(terminalNode.getSymbol());
    }

    // ******** Helper ************

    public static <T> Optional<T> visitIfPresent(AbstractParseTreeVisitor visitor, ParseTree context,
        Class<T> clazz) {
        return Optional.ofNullable(context)
            .map(visitor::visit)
            .map(clazz::cast);
    }

    public static <T> List<T> visit(AbstractParseTreeVisitor visitor, List<? extends ParseTree> contexts,
        Class<T> clazz) {
        return contexts.stream()
            .map(visitor::visit)
            .filter(Objects::nonNull)
            .map(clazz::cast)
            .collect(toList());
    }

    public static Identifier getIdentifier(ParserRuleContext ctx) {
        if (ctx == null) {
            return null;
        }
        //大小写忽略处理
        String text = ctx.getText();
        if (text.startsWith(REGEX)) {
            return getIdentifier(text, REGEX, ctx);
        }
        if (text.startsWith(DQUOTA)) {
            return getIdentifier(text, DQUOTA, ctx);
        }
        //if text not start with ' or ""
        return new Identifier(getLocation(ctx), getOrigin(ctx), text.toLowerCase(Locale.ROOT));
    }

    private static Identifier getIdentifier(String text, String regex, ParserRuleContext ctx) {
        text = text.substring(1, text.length() - 1).replaceAll(regex, StringUtils.EMPTY);
        return new Identifier(getLocation(ctx), getOrigin(ctx), text, true);
    }
}
