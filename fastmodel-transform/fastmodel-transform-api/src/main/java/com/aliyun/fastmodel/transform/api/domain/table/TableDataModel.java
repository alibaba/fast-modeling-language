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

package com.aliyun.fastmodel.transform.api.domain.table;

import java.util.List;
import java.util.Map;

import com.aliyun.fastmodel.transform.api.domain.DomainObject;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * TableDataModel
 *
 * @author panguanjing
 * @date 2020/9/18
 */
@Getter
@Setter
@ToString
public class TableDataModel implements DomainObject {
    private String database;
    private String tableCode;
    private String tableName;
    private String comment;
    private String tableType;
    private List<ColDataModel> cols;
    private List<ConstraintDataModel> constraints;
    private Map<String, String> tblProperties;

}
