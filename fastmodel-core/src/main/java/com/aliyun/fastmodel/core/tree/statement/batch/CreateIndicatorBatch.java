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

package com.aliyun.fastmodel.core.tree.statement.batch;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.BaseOperatorStatement;
import com.aliyun.fastmodel.core.tree.statement.batch.element.DateField;
import com.aliyun.fastmodel.core.tree.statement.batch.element.DefaultAdjunct;
import com.aliyun.fastmodel.core.tree.statement.batch.element.DimPathElement;
import com.aliyun.fastmodel.core.tree.statement.batch.element.DimTableElement;
import com.aliyun.fastmodel.core.tree.statement.batch.element.FromTableElement;
import com.aliyun.fastmodel.core.tree.statement.batch.element.IndicatorDefine;
import com.aliyun.fastmodel.core.tree.statement.batch.element.TimePeriodElement;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import lombok.Getter;

/**
 * create Indicator Batch
 *
 * @author panguanjing
 * @date 2021/2/22
 */
@Getter
public class CreateIndicatorBatch extends BaseOperatorStatement {

    private final List<IndicatorDefine> indicatorDefine;

    private final DefaultAdjunct defaultAdjunct;

    private final FromTableElement fromTableElement;

    private final DimTableElement dimTableElement;

    private final TimePeriodElement timePeriodElement;

    private final DimPathElement dimPathElement;

    private final DateField dateField;

    private final List<Property> propertyList;

    public CreateIndicatorBatch(QualifiedName qualifiedName,
                                List<IndicatorDefine> indicatorDefine,
                                DefaultAdjunct defaultAdjunct,
                                FromTableElement fromTableElement,
                                DimTableElement dimTableElement,
                                TimePeriodElement timePeriodElement,
                                DimPathElement dimPathElement,
                                DateField dateField,
                                List<Property> propertyList) {
        super(qualifiedName);
        this.indicatorDefine = indicatorDefine;
        this.defaultAdjunct = defaultAdjunct;
        this.fromTableElement = fromTableElement;
        this.dimTableElement = dimTableElement;
        this.timePeriodElement = timePeriodElement;
        this.dimPathElement = dimPathElement;
        this.dateField = dateField;
        this.propertyList = propertyList;
        setStatementType(StatementType.BATCH);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateIndicatorBatch(this, context);
    }

}
