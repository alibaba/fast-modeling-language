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

package com.aliyun.fastmodel.transform.api.domain.dqc;

import java.util.List;

import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.aliyun.fastmodel.transform.api.domain.dqc.constant.EntityLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * 定义实体规则模型对象
 *
 * @author panguanjing
 * @date 2021/5/31
 */
@Getter
@Setter
@ToString
public class EntityRules {

    /**
     * 表名
     */
    private String tableName;
    /**
     * 分区表达式
     */
    private String matchExpression;

    /**
     * 实体级别
     */
    private EntityLevel entityLevel;

    /**
     * 模型引擎名称
     */
    private DialectName dialectName;

    /**
     * 规则列表操作
     */
    private List<BaseRuleModel> ruleModelList;

}
