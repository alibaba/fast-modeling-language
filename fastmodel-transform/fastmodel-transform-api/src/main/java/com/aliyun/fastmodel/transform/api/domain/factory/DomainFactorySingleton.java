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

package com.aliyun.fastmodel.transform.api.domain.factory;

import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.transform.api.domain.factory.impl.TableDomainFactory;
import com.aliyun.fastmodel.transform.api.domain.table.TableDataModel;

/**
 * static method
 *
 * @author panguanjing
 * @date 2021/12/12
 */
public class DomainFactorySingleton {

    private static final TableDomainFactory INSTANCE = new TableDomainFactory();

    public static DomainFactory<CreateTable, TableDataModel> newTableFactory() {
        return INSTANCE;
    }
}
