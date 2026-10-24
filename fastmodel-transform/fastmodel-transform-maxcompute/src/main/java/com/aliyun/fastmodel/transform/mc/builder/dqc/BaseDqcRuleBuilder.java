/*
 * Copyright 2021 Alibaba Group Holding Ltd.
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

package com.aliyun.fastmodel.transform.mc.builder.dqc;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.atom.TableOrColumn;
import com.aliyun.fastmodel.core.tree.expr.literal.BaseLiteral;
import com.aliyun.fastmodel.core.tree.statement.dqc.check.BaseCheckElement;
import com.aliyun.fastmodel.core.tree.statement.rule.PartitionSpec;
import com.aliyun.fastmodel.core.tree.statement.rule.RuleDefinition;
import com.aliyun.fastmodel.core.tree.statement.rule.RuleGrade;
import com.aliyun.fastmodel.core.tree.statement.rule.RuleStrategy;
import com.aliyun.fastmodel.core.tree.statement.rule.function.BaseFunction;
import com.aliyun.fastmodel.core.tree.statement.rule.function.BaseFunctionName;
import com.aliyun.fastmodel.core.tree.statement.rule.function.VolFunction;
import com.aliyun.fastmodel.core.tree.statement.rule.function.column.ColumnFunction;
import com.aliyun.fastmodel.core.tree.statement.rule.strategy.DynamicStrategy;
import com.aliyun.fastmodel.core.tree.statement.rule.strategy.FixedStrategy;
import com.aliyun.fastmodel.core.tree.statement.rule.strategy.StrategyUtil;
import com.aliyun.fastmodel.core.tree.statement.rule.strategy.VolInterval;
import com.aliyun.fastmodel.core.tree.statement.rule.strategy.VolStrategy;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.domain.dqc.BaseRuleModel;
import com.aliyun.fastmodel.transform.api.domain.dqc.BaseRuleModel.Action;
import com.aliyun.fastmodel.transform.api.domain.dqc.RuleTemplate;
import com.aliyun.fastmodel.transform.api.domain.dqc.SelfCustomRuleModel;
import com.aliyun.fastmodel.transform.api.domain.dqc.TemplateRuleModel;
import com.aliyun.fastmodel.transform.api.domain.dqc.constant.BlockType;
import com.aliyun.fastmodel.transform.api.domain.dqc.constant.CheckerType;
import com.aliyun.fastmodel.transform.api.domain.dqc.constant.PredictType;
import com.aliyun.fastmodel.transform.api.domain.dqc.constant.PropertyType;
import com.aliyun.fastmodel.transform.api.domain.dqc.constant.RuleType;
import com.aliyun.fastmodel.transform.api.domain.dqc.constant.TemplateDefine;
import com.aliyun.fastmodel.transform.api.sqlconvert.SqlConvertFactory;
import com.aliyun.fastmodel.transform.api.sqlconvert.SqlConverter;
import com.aliyun.fastmodel.transform.mc.builder.BaseBuilder;
import com.aliyun.fastmodel.transform.mc.context.MaxComputeContext;
import com.aliyun.fastmodel.transform.mc.util.MaxComputeV1DataTypeUtil;
import com.google.common.collect.ImmutableList;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * 将一些通用方法进行抽象
 *
 * @author panguanjing
 * @date 2021/6/2
 */
public abstract class BaseDqcRuleBuilder extends BaseBuilder {

    public static final String NOTAPARTITIONTABLE = "NOTAPARTITIONTABLE";
    private final Set<BaseFunctionName> customRuleFunction;

    public static final String COLON = ":";

    private static final String COMMA = ",";

    public BaseDqcRuleBuilder() {
        customRuleFunction = new HashSet<>();
        customRuleFunction.add(BaseFunctionName.IN_TABLE);
    }

    /**
     * 转换下规则定义
     *
     * @param baseCheckElements
     * @return
     */
    protected List<RuleDefinition> toRuleDefinition(List<BaseCheckElement> baseCheckElements) {
        return baseCheckElements.stream().map(this::toRuleDefinition).collect(Collectors.toList());
    }

    protected RuleDefinition toRuleDefinition(BaseCheckElement baseCheckElement) {
        RuleDefinition ruleDefinition = RuleDefinition.builder()
            .enable(baseCheckElement.isEnable())
            .ruleGrade(BooleanUtils.isTrue(baseCheckElement.getEnforced()) ? RuleGrade.STRONG : RuleGrade.WEAK)
            .ruleName(baseCheckElement.getCheckName())
            .ruleStrategy(StrategyUtil.toStrategy(baseCheckElement.getBoolExpression()))
            .build();
        return ruleDefinition;
    }

    /**
     * 将ruleDefinition转换为BaseRuleModel
     *
     * @param ruleDefinitions
     * @param matchExpression
     * @param maxComputeContext
     * @return
     */
    public List<BaseRuleModel> toRuleModelList(List<RuleDefinition> ruleDefinitions, QualifiedName tableName,
        String matchExpression, MaxComputeContext maxComputeContext) {
        if (ruleDefinitions == null) {
            return ImmutableList.of();
        }
        return ruleDefinitions.stream().map(x -> {
            BaseRuleModel ruleModel = toBaseRuleModel(x, tableName, matchExpression, maxComputeContext);
            return ruleModel;
        }).collect(Collectors.toList());
    }

    public String toMatchExpression(List<PartitionSpec> partitionSpecList) {
        if (partitionSpecList == null || partitionSpecList.isEmpty()) {
            return NOTAPARTITIONTABLE;
        }
        return partitionSpecList.stream().map(x -> {
            BaseLiteral baseLiteral = x.getBaseLiteral();
            if (baseLiteral == null) {
                return String.format("%s=<%s>", x.getPartitionColName().getValue(), "[a-zA-Z0-9_-]*");
            } else {
                return String.format("%s=%s", x.getPartitionColName().getValue(), baseLiteral);
            }
        }).collect(Collectors.joining("/"));
    }

    public BaseRuleModel toBaseRuleModel(RuleDefinition ruleDefinition, QualifiedName tableName,
        String matchExpression, MaxComputeContext maxComputeContext) {
        RuleStrategy ruleStrategy = ruleDefinition.getRuleStrategy();
        BaseRuleModel baseRuleModel = null;
        if (ruleStrategy == null) {
            baseRuleModel = new BaseRuleModel();
            String value = ruleDefinition.getRuleName().getValue();
            baseRuleModel.setProperty(value);
            baseRuleModel.setAction(Action.UPDATE);
            return baseRuleModel;
        }
        BaseFunction baseFunction = ruleStrategy.getFunction();
        BaseFunctionName baseFunctionName = baseFunction.funcName();
        boolean isTemplateRule = false;
        if (customRuleFunction.contains(baseFunctionName)) {
            baseRuleModel = new SelfCustomRuleModel();
            baseRuleModel.setCheckerType(CheckerType.FIX_STRATEGY_CHECK);
            String ruleValue = toSql(baseFunction, tableName, matchExpression, maxComputeContext);
            baseRuleModel.setRuleValue(ruleValue);
        } else {
            baseRuleModel = new TemplateRuleModel();
            isTemplateRule = true;
        }
        fill(baseRuleModel, ruleDefinition);
        if (ruleStrategy instanceof FixedStrategy) {
            FixedStrategy fixedStrategy = (FixedStrategy)ruleStrategy;
            if (isTemplateRule) {
                RuleTemplate template = getTemplate(baseFunction, CheckerType.FIX_STRATEGY_CHECK);
                baseRuleModel.setRuleValue(template.getTemplateCode() + COLON + fixedStrategy.getExpectValue().toString());
            }
            BaseLiteral expectValue = fixedStrategy.getExpectValue();
            baseRuleModel.setExpectValue(expectValue.toString());
            baseRuleModel.setOperator(fixedStrategy.getComparisonOperator().getCode());
        } else if (ruleStrategy instanceof VolStrategy) {
            VolStrategy volStrategy = (VolStrategy)ruleStrategy;
            VolFunction volFunction = volStrategy.getVolFunction();
            VolInterval volInterval = volStrategy.getVolInterval();
            if (isTemplateRule) {
                RuleTemplate template = getTemplate(baseFunction, CheckerType.VOL_STRATEGY_CHECK);
                baseRuleModel.setRuleValue(template.getTemplateCode());
            }
            baseRuleModel.setTrend(volStrategy.getVolOperator().name().toLowerCase(Locale.ROOT));
            baseRuleModel.setWarningThreshold(volInterval.getStart().intValue());
            baseRuleModel.setCriticalThreshold(volInterval.getEnd().intValue());
        } else if (ruleStrategy instanceof DynamicStrategy) {
            DynamicStrategy dynamicStrategy = (DynamicStrategy)ruleStrategy;
            baseRuleModel.setPredictType(PredictType.DYNAMIC);
            if (isTemplateRule) {
                RuleTemplate template = getTemplate(baseFunction, CheckerType.DYNAMIC_STRATEGY_CHECK);
                baseRuleModel.setRuleValue(template.getTemplateCode());
            }
            baseRuleModel.setDynamicCheckSamples(dynamicStrategy.getNumberValue());
            baseRuleModel.setOperator("auto");
        }
        return baseRuleModel;
    }

    private String toSql(BaseFunction baseFunction, QualifiedName tableName, String matchExpression, MaxComputeContext maxComputeContext) {
        SqlConverter sqlConverter = SqlConvertFactory.getInstance().get(DialectMeta.getMaxCompute());
        return sqlConverter.convert(tableName, baseFunction, matchExpression, maxComputeContext);
    }

    private void fill(BaseRuleModel ruleModel, RuleDefinition ruleDefinition) {
        ruleModel.setRuleName(ruleDefinition.getRuleName().getValue());
        boolean enable = ruleDefinition.isEnable();
        if (enable) {
            ruleModel.setAction(Action.ADD);
        } else {
            ruleModel.setAction(Action.DELETE);
        }
        ruleModel.setBlockType(BlockType.getByGrade(ruleDefinition.getRuleGrade()));
        ruleModel.setPredictType(PredictType.NOT_DYNAMIC);
        fillProperty(ruleDefinition.getRuleStrategy().getFunction(), ruleModel);
        Comment comment = ruleDefinition.getComment();
        ruleModel.setComment(comment != null ? comment.getComment() : null);
    }

    /**
     * 获取属性的设置
     *
     * @param x
     * @param ruleModel
     * @return
     */
    private void fillProperty(BaseFunction function,
        BaseRuleModel ruleModel) {
        if (ruleModel.getRuleType() == RuleType.SLEF_CUSTOM) {
            ruleModel.setProperty(PropertyType.USER_DEFINED.name());
            ruleModel.setPropertyType(PropertyType.USER_DEFINED.getValue());
            return;
        }
        BaseFunctionName baseFunctionName = function.funcName();
        if (baseFunctionName.isTableFunction()) {
            List<BaseExpression> arguments = function.arguments();
            if(CollectionUtils.isNotEmpty(arguments)) {
                ruleModel.setProperty(StringUtils.join(arguments.stream().map(BaseExpression::getOrigin).collect(Collectors.toList()), COMMA));
            } else {
                ruleModel.setProperty(baseFunctionName.name().toLowerCase(Locale.ROOT));
            }
            ruleModel.setPropertyType(PropertyType.TABLE.getValue());
            return;
        }
        if (baseFunctionName.isColumnFunction()) {
            List<BaseExpression> arguments = function.arguments();
            TableOrColumn t = (TableOrColumn)arguments.get(0);
            String suffix = t.getQualifiedName().getSuffix();
            ruleModel.setProperty(suffix);
            ColumnFunction columnFunction = (ColumnFunction)function;
            BaseDataType baseDataType = columnFunction.getBaseDataType();
            if (baseDataType == null) {
                //如果为空，那么默认设置为bigint
                ruleModel.setPropertyType(DataTypeEnums.BIGINT.name());
            } else {
                IDataTypeName convert = MaxComputeV1DataTypeUtil.convertIfNecessary(baseDataType.getTypeName());
                if (convert != null) {
                    ruleModel.setPropertyType(convert.getValue());
                } else {
                    ruleModel.setPropertyType(baseDataType.toString());
                }
            }
            return;
        }
        if (baseFunctionName.isVolFunction()) {
            VolFunction v = (VolFunction)function;
            fillProperty(v.getBaseFunction(), ruleModel);
            return;
        }
        throw new IllegalArgumentException("unSupported function with name:" + baseFunctionName);
    }

    private RuleTemplate getTemplate(BaseFunction x, CheckerType checkerType) {
        TemplateDefine define = TemplateDefine.getTemplateIdByFunction(x, checkerType);
        RuleTemplate ruleTemplate = new RuleTemplate();
        ruleTemplate.setTemplateCode(define.getTemplateCode());
        ruleTemplate.setTemplateName(define.getRuleName());
        return ruleTemplate;
    }
}
