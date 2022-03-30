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

import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.aliyun.transform.zen.element.ColElement;
import com.aliyun.aliyun.transform.zen.parser.tree.BaseZenNode;
import com.aliyun.fastmodel.core.exception.ParseException;
import com.aliyun.fastmodel.core.parser.DomainLanguage;
import com.aliyun.fastmodel.core.parser.FastModelParserFactory;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.google.auto.service.AutoService;

/**
 * ZenNodeConverterImpl
 *
 * @author panguanjing
 * @date 2021/7/16
 */
@AutoService(ZenNodeConverter.class)
public class ZenNodeColumnDefinitionConverter implements ZenNodeConverter<ColumnDefinition, ZenNodeConvertContext> {
    @Override
    public List<ColumnDefinition> convert(BaseZenNode baseZenNode, ZenNodeConvertContext context) {
        ColumnConverterVisitor columnConverterVisitor = new ColumnConverterVisitor();
        ColElement accept = baseZenNode.accept(columnConverterVisitor, context);
        List<ColElement> expand = accept.expand();
        return expand.stream().map(x -> {
            BaseDataType dataType = getDataType(x);
            Comment comment = getComment(x);
            return ColumnDefinition.builder()
                .colName(new Identifier(x.getColName()))
                .dataType(dataType)
                .comment(comment).build();
        }).collect(Collectors.toList());
    }

    private Comment getComment(ColElement x) {
        List<String> classList = x.getClassList();
        Comment comment = new Comment(x.getColName());
        if (classList == null || classList.isEmpty() || classList.size() == 1) {
            return comment;
        }
        String text = classList.get(1);
        return new Comment(text);
    }

    private BaseDataType getDataType(ColElement x) {
        List<String> classList = x.getClassList();
        BaseDataType baseDataType = DataTypeUtil.simpleType(DataTypeEnums.STRING);
        if (classList == null || classList.isEmpty()) {
            return baseDataType;
        }
        String text = classList.get(0);
        try {
            baseDataType = FastModelParserFactory.getInstance()
                .get()
                .parseDataType(new DomainLanguage(text));
            return baseDataType;
        } catch (ParseException e) {
            return DataTypeUtil.simpleType(DataTypeEnums.STRING);
        }
    }
}
