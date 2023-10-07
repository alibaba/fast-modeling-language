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

package com.aliyun.fastmodel.core.parser;

import java.util.List;

import com.aliyun.fastmodel.core.exception.ParseException;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.atom.TableOrColumn;

/**
 * DSL解析器，提供统一接口
 *
 * @author panguanjing
 * @date 2020/9/1
 */
public interface FastModelParser extends LanguageParser<Node, Void> {

    @Override
    default Node parseNode(String text, Void context) throws ParseException {
        return parseStatement(text);
    }

    /**
     * 执行的parse生成所需要的语句内容
     *
     * @param domainLanguage {@link DomainLanguage}
     * @return {@link BaseStatement}
     * @throws ParseException 如果语句不符合定义
     */
    BaseStatement parse(DomainLanguage domainLanguage) throws ParseException;

    /**
     * Parse Statement
     *
     * @param text DSL text
     * @return T
     * @throws ParseException parse error
     */
    <T extends BaseStatement> T parseStatement(String text) throws ParseException;

    /**
     * 解析支持多个语句
     *
     * @param script 脚本
     * @return {@link BaseStatement}
     * @throws ParseException 解析异常
     */
    List<BaseStatement> multiParse(DomainLanguage script) throws ParseException;

    /**
     * 支持解析表达式处理
     *
     * @param expr 表达式的处理
     * @return {@link BaseExpression}
     * @throws ParseException dsl异常
     */
    BaseExpression parseExpr(DomainLanguage expr) throws ParseException;

    /**
     * 解析表达式，支持泛型, 不需要强转
     *
     * @param text¬ 表达式
     * @param <T>   T
     * @return T
     * @throws ParseException 解析异常
     */
    <T> T parseExpression(String text) throws ParseException;

    /**
     * 支持类型表达式的解析
     *
     * @param dataTypeExpr 数据类型表达式
     * @return {@link BaseDataType}
     * @throws ParseException 解析异常
     */
    BaseDataType parseDataType(DomainLanguage dataTypeExpr) throws ParseException;

    /**
     * 从表达式中获取TableOrColumn
     *
     * @param domainLanguage 语言
     * @return {@link TableOrColumn}
     * @throws ParseException dsl
     */
    List<TableOrColumn> extract(DomainLanguage domainLanguage) throws ParseException;

}
