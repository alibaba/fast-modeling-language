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

package com.aliyun.fastmodel.transform.flink.parser.tree.datatype;

import java.util.List;
import java.util.Objects;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.transform.flink.context.FlinkTransformContext;
import com.aliyun.fastmodel.transform.flink.parser.visitor.FlinkExpressionAstVisitor;
import com.aliyun.fastmodel.transform.flink.parser.visitor.FlinkAstVisitor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

/**
 * @author 子梁
 * @date 2024/5/17
 */
@Getter
@EqualsAndHashCode(callSuper = false)
public class FlinkRawDataType extends BaseDataType {

    /**
     * row data type qulifiedname
     */
    private final List<QualifiedName> qualifiedNames;

    public FlinkRawDataType(NodeLocation location, String origin,
                       List<QualifiedName> qualifiedNames) {
        super(location, origin);
        this.qualifiedNames = qualifiedNames;
    }

    public FlinkRawDataType(List<QualifiedName> qualifiedNames) {
        this.qualifiedNames = qualifiedNames;
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        FlinkAstVisitor<R, C> flinkAstVisitor = (FlinkAstVisitor<R, C>)visitor;
        return flinkAstVisitor.visitFlinkRawDataType(this, context);
    }

    @Override
    public List<? extends Node> getChildren() {
        return qualifiedNames;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {return true;}
        if (o == null || getClass() != o.getClass()) {return false;}
        FlinkRawDataType flinkRawDataType = (FlinkRawDataType)o;
        boolean e = StringUtils.equalsIgnoreCase(this.getTypeName().getValue(), flinkRawDataType.getTypeName().getValue());
        if (!e) {
            return false;
        }
        return Objects.equals(qualifiedNames, flinkRawDataType.getQualifiedNames());
    }

    @Override
    public IDataTypeName getTypeName() {
        return FlinkDataTypeName.RAW;
    }

    @Override
    public String toString() {
        return new FlinkExpressionAstVisitor(FlinkTransformContext.builder().build()).process(this);
    }
}
