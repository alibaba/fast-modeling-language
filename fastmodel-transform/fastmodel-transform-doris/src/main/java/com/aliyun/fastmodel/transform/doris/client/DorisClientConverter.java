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

package com.aliyun.fastmodel.transform.doris.client;

import java.util.List;

import com.aliyun.fastmodel.core.parser.LanguageParser;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.transform.api.client.PropertyConverter;
import com.aliyun.fastmodel.transform.api.client.dto.constraint.Constraint;
import com.aliyun.fastmodel.transform.api.extension.client.constraint.DistributeClientConstraint;
import com.aliyun.fastmodel.transform.api.extension.client.converter.ExtensionClientConverter;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.desc.DistributeNonKeyConstraint;
import com.aliyun.fastmodel.transform.doris.context.DorisContext;
import com.aliyun.fastmodel.transform.doris.format.DorisOutVisitor;
import com.aliyun.fastmodel.transform.doris.parser.DorisLanguageParser;
import com.aliyun.fastmodel.transform.doris.parser.tree.DorisDataTypeName;
import com.google.common.collect.Lists;

/**
 * DorisClientConverter
 *
 * @author panguanjing
 * @date 2024/1/21
 */
public class DorisClientConverter extends ExtensionClientConverter<DorisContext> {

    private final DorisLanguageParser dorisLanguageParser;

    private final DorisPropertyConverter dorisPropertyConverter;

    public DorisClientConverter() {
        dorisLanguageParser = new DorisLanguageParser();
        dorisPropertyConverter = new DorisPropertyConverter();
    }

    @Override
    public IDataTypeName getDataTypeName(String dataTypeName) {
        return DorisDataTypeName.getByValue(dataTypeName);
    }

    @Override
    public LanguageParser getLanguageParser() {
        return this.dorisLanguageParser;
    }

    @Override
    public String getRaw(Node node) {
        DorisOutVisitor dorisOutVisitor = new DorisOutVisitor(DorisContext.builder().build());
        node.accept(dorisOutVisitor, 0);
        return dorisOutVisitor.getBuilder().toString();
    }

    @Override
    protected List<Constraint> toOutlineConstraint(CreateTable createTable) {
        List<Constraint> outlineConstraint = super.toOutlineConstraint(createTable);
        if (createTable.isConstraintEmpty()) {
            return outlineConstraint;
        }
        List<BaseConstraint> constraintStatements = createTable.getConstraintStatements();
        if (outlineConstraint == null || outlineConstraint.isEmpty()) {
            outlineConstraint = Lists.newArrayList();
        }
        if (!constraintStatements.isEmpty()) {
            for (BaseConstraint baseConstraint : constraintStatements) {
                convertConstraint(baseConstraint, outlineConstraint);
            }
        }
        return outlineConstraint;
    }

    private void convertConstraint(BaseConstraint baseConstraint, List<Constraint> outlineConstraint) {
        if (baseConstraint instanceof DistributeNonKeyConstraint) {
            DistributeNonKeyConstraint distributeConstraint = (DistributeNonKeyConstraint)baseConstraint;
            DistributeClientConstraint starRocksDistributeConstraint = toDistribute(distributeConstraint);
            outlineConstraint.add(starRocksDistributeConstraint);
            return;
        }
    }

    @Override
    public PropertyConverter getPropertyConverter() {
        return dorisPropertyConverter;
    }
}
