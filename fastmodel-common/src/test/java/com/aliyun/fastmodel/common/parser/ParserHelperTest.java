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

package com.aliyun.fastmodel.common.parser;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.literal.DecimalLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.DoubleLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.google.common.collect.ImmutableList;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonToken;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.AbstractParseTreeVisitor;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.antlr.v4.runtime.tree.TerminalNodeImpl;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/11/26
 */
public class ParserHelperTest {

    @Test
    public void testGetOrigin() {
        ParserRuleContext parserRuleContext = Mockito.mock(ParserRuleContext.class);
        prepare(parserRuleContext);
        String result = ParserHelper.getOrigin(parserRuleContext);
        assertEquals("result", result);
    }

    private void prepare(ParserRuleContext parserRuleContext) {
        Token start = Mockito.mock(Token.class);
        given(parserRuleContext.getStart()).willReturn(start);
        given(start.getStartIndex()).willReturn(0);
        Token end = Mockito.mock(Token.class);
        given(parserRuleContext.getStop()).willReturn(end);
        given(end.getStopIndex()).willReturn(0);
        CharStream charStream = Mockito.mock(CharStream.class);
        given(start.getInputStream()).willReturn(charStream);
        given(charStream.getText(any(Interval.class))).willReturn("result");
    }

    @Test
    public void testGetOrigin2() {
        Token token = getToken();
        String result = ParserHelper.getOrigin(token);
        assertEquals("result", result);
    }

    private Token getToken() {
        Token token = Mockito.mock(Token.class);
        CharStream charStream = Mockito.mock(CharStream.class);
        given(token.getInputStream()).willReturn(charStream);
        given(charStream.getText(any(Interval.class))).willReturn("result");
        return token;
    }

    @Test
    public void testGetOrigin3() {
        TerminalNode terminalNode = Mockito.mock(TerminalNode.class);
        Token token = getToken();
        given(terminalNode.getSymbol()).willReturn(token);
        String result = ParserHelper.getOrigin(terminalNode);
        assertEquals("result", result);
    }

    @Test
    public void testGetLocation() {
        ParserRuleContext parserRuleContext = Mockito.mock(ParserRuleContext.class);
        Token token = getToken();
        given(parserRuleContext.getStart()).willReturn(token);
        NodeLocation result = ParserHelper.getLocation(parserRuleContext);
        assertEquals(result.getColumn(), 1);
    }

    @Test
    public void testGetLocation2() {
        NodeLocation result = ParserHelper.getLocation(getToken());
        assertEquals(1, result.getColumn());
    }

    @Test
    public void testGetLocation3() {
        TerminalNode mock = Mockito.mock(TerminalNode.class);
        Token token = getToken();
        given(mock.getSymbol()).willReturn(token);
        NodeLocation result = ParserHelper.getLocation(mock);
        assertEquals(result.getColumn(), 1);
    }

    @Test
    public void testVisitIfPresent() {
        AbstractParseTreeVisitor mock = Mockito.mock(AbstractParseTreeVisitor.class);
        given(mock.visit(any(ParseTree.class))).willReturn(new Identifier("abc"));
        Optional<Identifier> result = ParserHelper.visitIfPresent(mock, new TerminalNodeImpl(new CommonToken(0)),
            Identifier.class);
        assertEquals(new Identifier("abc"), result.get());
    }

    @Test
    public void testVisit() throws Exception {
        List<TerminalNode> list = ImmutableList.of(new TerminalNodeImpl(new CommonToken(0)));
        AbstractParseTreeVisitor mock = Mockito.mock(AbstractParseTreeVisitor.class);
        given(mock.visit(any(ParseTree.class))).willReturn(new Identifier("abc"));
        List<Identifier> result = ParserHelper.visit(mock,
            list, Identifier.class);
        assertEquals(result.size(), 1);

    }

    @Test
    public void testGetNumber() {
        LongLiteral liter = new LongLiteral("1");
        Number number = ParserHelper.getNumber(liter);
        assertEquals(number.longValue(), 1L);

        DoubleLiteral doubleLiteral = new DoubleLiteral("1.12");
        Number number1 = ParserHelper.getNumber(doubleLiteral);
        double v = number1.doubleValue();
        Double value = doubleLiteral.getValue();
        assertEquals(value.doubleValue(), v, 0.0);

        DecimalLiteral decimalLiteral = new DecimalLiteral("12134234234234234234");
        Number number2 = ParserHelper.getNumber(decimalLiteral);
        assertEquals(decimalLiteral.getNumber(), ((BigDecimal)number2).toString());

        StringLiteral stringLiteral = new StringLiteral("1234");
        Number number3 = ParserHelper.getNumber(stringLiteral);
        assertSame(number3.getClass(), BigDecimal.class);
    }

    @Test
    public void testLower() {
        ParserRuleContext parserRuleContext = new ParserRuleContext();
        parserRuleContext.start = new CommonToken(0, "`");
        parserRuleContext.stop = new CommonToken(0, "`");
        Token symbol = new CommonToken(0, "`HOUR`");
        Token t1 = new CommonToken(symbol);
        parserRuleContext.addChild(t1);
        Identifier identifier = ParserHelper.getIdentifier(parserRuleContext);
        assertEquals(identifier.getValue(), "HOUR");
    }

    @Test
    public void testLowerWithNo() {
        ParserRuleContext parserRuleContext = new ParserRuleContext();
        parserRuleContext.start = new CommonToken(0, "`");
        parserRuleContext.stop = new CommonToken(0, "`");
        Token symbol = new CommonToken(0, "HOUR");
        Token t1 = new CommonToken(symbol);
        parserRuleContext.addChild(t1);
        Identifier identifier = ParserHelper.getIdentifier(parserRuleContext);
        assertEquals(identifier.getValue(), "hour");
    }
}

//Generated with love by TestMe :) Please report issues and submit feature requests at: http://weirddev
// .com/forum#!/testme