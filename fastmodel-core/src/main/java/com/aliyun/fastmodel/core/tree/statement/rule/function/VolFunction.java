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

package com.aliyun.fastmodel.core.tree.statement.rule.function;

import java.util.ArrayList;
import java.util.List;

import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * 波动率的函数
 *
 * @author panguanjing
 * @date 2021/5/29
 */
@Getter
@EqualsAndHashCode(callSuper = true)
public class VolFunction extends BaseFunction {

    /**
     * 具体的function
     */
    private final BaseFunction baseFunction;

    /**
     * 日期的列表
     */
    private final List<BaseExpression> dateList;

    public VolFunction(NodeLocation location,
                       BaseFunction baseFunction,
                       List<BaseExpression> dateList) {
        super(location);
        this.baseFunction = baseFunction;
        this.dateList = dateList;
    }

    public VolFunction(BaseFunction baseFunction,
                       List<BaseExpression> dateList) {
        this.baseFunction = baseFunction;
        this.dateList = dateList;
    }

    @Override
    public BaseFunctionName funcName() {
        return BaseFunctionName.VOL;
    }

    @Override
    public List<BaseExpression> arguments() {
        List<BaseExpression> list = new ArrayList<>();
        list.add(baseFunction);
        list.addAll(dateList);
        return list;
    }
}
