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

package com.aliyun.fastmodel.transform.plantuml.parser;

import java.util.HashMap;
import java.util.Map;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.statement.indicator.CreateIndicator;
import com.aliyun.fastmodel.core.tree.statement.script.RefRelation;
import com.aliyun.fastmodel.core.tree.statement.table.CreateDimTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateFactTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.transform.plantuml.parser.impl.IndicatorFragmentParser;
import com.aliyun.fastmodel.transform.plantuml.parser.impl.RefRelationFragmentParser;
import com.aliyun.fastmodel.transform.plantuml.parser.impl.TableFragmentParser;

/**
 * 分段factory内容
 *
 * @author panguanjing
 * @date 2020/9/18
 */
public class FragmentFactory {

    Map<String, FragmentParser> maps = new HashMap<>();

    private FragmentFactory() {
        maps.put(CreateDimTable.class.getName(), new TableFragmentParser());
        maps.put(CreateFactTable.class.getName(), new TableFragmentParser());
        maps.put(CreateTable.class.getName(), new TableFragmentParser());
        maps.put(CreateIndicator.class.getName(), new IndicatorFragmentParser());
        maps.put(RefRelation.class.getName(), new RefRelationFragmentParser());
    }

    public static FragmentFactory getInstance() {
        return new FragmentFactory();
    }

    public FragmentParser get(BaseStatement statement) {
        return maps.get(statement.getClass().getName());
    }
}
