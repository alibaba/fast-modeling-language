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

package com.aliyun.fastmodel.transform.plantuml.parser.impl;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.statement.indicator.CreateIndicator;
import com.aliyun.fastmodel.transform.api.domain.indicator.IndicatorDataModel;
import com.aliyun.fastmodel.transform.plantuml.exception.VisualParseException;
import com.aliyun.fastmodel.transform.plantuml.parser.Fragment;
import com.aliyun.fastmodel.transform.plantuml.parser.FragmentImpl;
import com.aliyun.fastmodel.transform.plantuml.parser.FragmentParser;
import com.aliyun.fastmodel.transform.template.BaseFmlTemplateFactory;
import com.aliyun.fastmodel.transform.template.FmlTemplate;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/9/21
 */
public class IndicatorFragmentParser implements FragmentParser {

    private FmlTemplate fmlTemplate;

    public IndicatorFragmentParser() {
        fmlTemplate = BaseFmlTemplateFactory.getInstance().getTemplate("fragment/indicator.ftl");
    }

    @Override
    public Fragment parse(BaseStatement statement) throws VisualParseException {
        CreateIndicator createIndicatorStatement = (CreateIndicator)statement;
        IndicatorDataModel indicatorDataModel = new IndicatorDataModel();
        Comment comment = createIndicatorStatement.getComment();
        indicatorDataModel.setComment(comment != null ? comment.getComment() : null);
        indicatorDataModel.setDataType(createIndicatorStatement.getDataType().toString());
        indicatorDataModel.setIndicatorName(createIndicatorStatement.getIdentifier());
        BaseExpression indicatorExpr = createIndicatorStatement.getIndicatorExpr();
        if (indicatorExpr != null) {
            indicatorDataModel.setExpr(indicatorExpr.toString());
        }
        try {
            String process = fmlTemplate.process(indicatorDataModel);
            FragmentImpl fragment = new FragmentImpl();
            fragment.setContent(process);
            return fragment;
        } catch (Exception e) {
            throw new VisualParseException("process error:", e);
        }
    }
}
