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
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.transform.api.domain.factory.impl.TableDomainFactory;
import com.aliyun.fastmodel.transform.api.domain.table.TableDataModel;
import com.aliyun.fastmodel.transform.plantuml.parser.Fragment;
import com.aliyun.fastmodel.transform.plantuml.parser.FragmentImpl;
import com.aliyun.fastmodel.transform.plantuml.parser.FragmentParser;
import com.aliyun.fastmodel.transform.template.BaseFmlTemplateFactory;
import com.aliyun.fastmodel.transform.template.FmlTemplate;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/9/18
 */
public class TableFragmentParser implements FragmentParser {

    private final FmlTemplate template;

    private TableDomainFactory tableDomainFactory;

    public TableFragmentParser() {
        template = BaseFmlTemplateFactory.getInstance().getTemplate("fragment/table.ftl");
        tableDomainFactory = new TableDomainFactory();
    }

    @Override
    public Fragment parse(BaseStatement statement) {
        CreateTable createTableStatement = (CreateTable)statement;
        TableDataModel dataModel = tableDomainFactory.create(createTableStatement);
        String process = template.process(dataModel);
        FragmentImpl fragment = new FragmentImpl();
        fragment.setContent(process);
        return fragment;
    }
}
