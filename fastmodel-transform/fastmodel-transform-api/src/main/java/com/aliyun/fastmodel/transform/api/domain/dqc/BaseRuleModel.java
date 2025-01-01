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

import com.aliyun.fastmodel.transform.api.domain.DomainObject;
import com.aliyun.fastmodel.transform.api.domain.dqc.constant.BlockType;
import com.aliyun.fastmodel.transform.api.domain.dqc.constant.CheckerType;
import com.aliyun.fastmodel.transform.api.domain.dqc.constant.PredictType;
import com.aliyun.fastmodel.transform.api.domain.dqc.constant.RuleType;
import lombok.Getter;
import lombok.Setter;

/**
 * BaseRuleModel
 *
 * @author panguanjing
 * @date 2021/5/31
 */
@Getter
@Setter
public class BaseRuleModel implements DomainObject {

    public static enum Action {
        /**
         * 添加规则
         */
        ADD,
        /**
         * 更新规则
         */
        UPDATE,
        /**
         * 删除规则
         */
        DELETE
    }

    private RuleType ruleType;

    /**
     * 操作类型
     */
    private Action action;

    /**
     * 规则级别
     */
    private BlockType blockType;

    /**
     * 规则名,唯一
     */
    private String ruleName;

    /**
     * 规则内容，如果是模板是模板Code，如果是自定义规则，那么是一个sql
     */
    private String ruleValue;

    /**
     * 原来的property
     */
    private String oldProperty;

    /**
     * 实体属性
     */
    private String property;

    /**
     * 实体属性类型
     */
    private String propertyType;

    /**
     * 规则描述
     */
    private String comment;

    /**
     * 比对操作符
     */
    private String operator;

    /**
     * 期望值
     **/
    private String expectValue;
    /**
     * 趋势操作符
     */
    private String trend;
    /**
     * 警告
     */
    private Integer warningThreshold;
    /**
     * crital
     */
    private Integer criticalThreshold;

    /**
     * 动态阀值
     */
    private PredictType predictType;

    /**
     * 动态阀值校验值
     */
    private Integer dynamicCheckSamples;

    /**
     * 检查类型
     */
    private CheckerType checkerType;

}
