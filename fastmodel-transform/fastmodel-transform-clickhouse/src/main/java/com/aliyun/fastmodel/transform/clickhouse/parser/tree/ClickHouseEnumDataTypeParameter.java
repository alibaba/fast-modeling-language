/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.clickhouse.parser.tree;

import java.util.List;

import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeParameter;
import com.aliyun.fastmodel.core.tree.expr.literal.BaseLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.google.common.collect.ImmutableList;
import lombok.Getter;

/**
 * ClickHouseEnumDataTypeParameter
 *
 * @author panguanjing
 * @date 2022/7/10
 */
@Getter
public class ClickHouseEnumDataTypeParameter extends DataTypeParameter {

    private final StringLiteral name;

    private final BaseLiteral baseLiteral;

    public ClickHouseEnumDataTypeParameter(StringLiteral name, BaseLiteral baseLiteral) {
        this.name = name;
        this.baseLiteral = baseLiteral;
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of();
    }

}
