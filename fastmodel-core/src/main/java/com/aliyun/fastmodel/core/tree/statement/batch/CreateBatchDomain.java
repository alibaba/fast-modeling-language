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

import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.BaseCreate;
import com.aliyun.fastmodel.core.tree.statement.batch.element.DomainElement;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import com.aliyun.fastmodel.core.tree.statement.element.CreateElement;
import lombok.Getter;

/**
 * CreateBatchDomain
 *
 * @author panguanjing
 * @date 2021/4/8
 */
@Getter
public class CreateBatchDomain extends BaseCreate {

    private final List<DomainElement> domainElements;

    public CreateBatchDomain(QualifiedName qualifiedName,
                             List<DomainElement> domainElements,
                             List<Property> properties) {
        super(CreateElement.builder().qualifiedName(qualifiedName).properties(properties).build());
        this.domainElements = domainElements;
        setStatementType(StatementType.BATCH);
    }
}
