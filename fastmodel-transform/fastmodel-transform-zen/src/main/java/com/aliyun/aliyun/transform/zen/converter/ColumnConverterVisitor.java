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

package com.aliyun.aliyun.transform.zen.converter;

import com.aliyun.aliyun.transform.zen.element.ColElement;
import com.aliyun.aliyun.transform.zen.parser.BaseZenAstVisitor;
import com.aliyun.aliyun.transform.zen.parser.tree.AtomicZenExpression;
import com.aliyun.aliyun.transform.zen.parser.tree.AttributeZenExpression;
import com.aliyun.aliyun.transform.zen.parser.tree.BaseZenExpression;
import com.aliyun.aliyun.transform.zen.parser.tree.BrotherZenExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

/**
 * ZenNodeConverterVisitor
 *
 * @author panguanjing
 * @date 2021/7/16
 */
@Getter
public class ColumnConverterVisitor extends BaseZenAstVisitor<ColElement, ZenNodeConvertContext> {

    public static final String DOLLAR_CHAR = "$";

    @Override
    public ColElement visitAttributeZenExpression(AttributeZenExpression attributeZenExpression,
                                                  ZenNodeConvertContext context) {
        BaseZenExpression baseZenExpression = attributeZenExpression.getBaseZenExpression();
        ColElement process = process(baseZenExpression, context);
        Identifier identifier = attributeZenExpression.getIdentifier();
        String value = null;
        if (identifier != null) {
            value = identifier.getValue();
        } else if (attributeZenExpression.getStringLiteral() != null) {
            value = attributeZenExpression.getStringLiteral().getValue();
        }
        process.addAttributeName(value);
        return process;
    }

    @Override
    public ColElement visitBrotherZenExpression(BrotherZenExpression brotherZenExpression,
                                                ZenNodeConvertContext context) {
        BaseZenExpression left = brotherZenExpression.getLeft();
        ColElement colElement = process(left, context);
        ColElement process = process(brotherZenExpression.getRight(), context);
        return colElement.merge(process);
    }

    @Override
    public ColElement visitAtomicZenExpression(AtomicZenExpression atomicZenExpression,
                                               ZenNodeConvertContext context) {
        Identifier identifier = atomicZenExpression.getIdentifier();
        Long numberValue = atomicZenExpression.getNumberValue();
        String value = identifier.getValue();
        ColElement colElement = new ColElement();
        if (numberValue != null) {
            int indexOf = value.indexOf(DOLLAR_CHAR);
            if (indexOf > -1) {
                String substring = value.substring(0, value.length() - 1);
                String newColName = StringUtils.replace(value, DOLLAR_CHAR, "1");
                colElement.setColName(newColName);
                for (int i = 1; i < numberValue.intValue(); i++) {
                    int start = i + 1;
                    String col = StringUtils.replace(value, DOLLAR_CHAR, String.valueOf(start));
                    ColElement subColElement = new ColElement();
                    subColElement.setColName(col);
                    colElement.merge(subColElement);
                }
            } else {
                colElement.setElementName(value + "1");
                for (int i = 1; i < numberValue.intValue(); i++) {
                    ColElement subColElement = new ColElement();
                    subColElement.setColName(value + (i + 1));
                    colElement.merge(subColElement);
                }
            }
        } else {
            colElement.setColName(identifier.getValue());
        }
        return colElement;
    }

}
