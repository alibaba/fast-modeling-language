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

package com.aliyun.fastmodel.core.tree;

import com.aliyun.fastmodel.core.tree.expr.atom.FunctionCall;
import com.aliyun.fastmodel.core.tree.statement.BaseCommandStatement;
import com.aliyun.fastmodel.core.tree.statement.BaseCreate;
import com.aliyun.fastmodel.core.tree.statement.BaseDrop;
import com.aliyun.fastmodel.core.tree.statement.BaseOperatorStatement;
import com.aliyun.fastmodel.core.tree.statement.BaseQueryStatement;
import com.aliyun.fastmodel.core.tree.statement.BaseRename;
import com.aliyun.fastmodel.core.tree.statement.BaseSetAliasedName;
import com.aliyun.fastmodel.core.tree.statement.BaseSetComment;
import com.aliyun.fastmodel.core.tree.statement.BaseSetProperties;
import com.aliyun.fastmodel.core.tree.statement.BaseUnSetProperties;
import com.aliyun.fastmodel.core.tree.statement.adjunct.CreateAdjunct;
import com.aliyun.fastmodel.core.tree.statement.adjunct.DropAdjunct;
import com.aliyun.fastmodel.core.tree.statement.adjunct.RenameAdjunct;
import com.aliyun.fastmodel.core.tree.statement.adjunct.SetAdjunctComment;
import com.aliyun.fastmodel.core.tree.statement.adjunct.SetAdjunctProperties;
import com.aliyun.fastmodel.core.tree.statement.batch.AbstractBatchElement;
import com.aliyun.fastmodel.core.tree.statement.batch.CreateIndicatorBatch;
import com.aliyun.fastmodel.core.tree.statement.batch.element.DateField;
import com.aliyun.fastmodel.core.tree.statement.batch.element.DefaultAdjunct;
import com.aliyun.fastmodel.core.tree.statement.batch.element.DimPathElement;
import com.aliyun.fastmodel.core.tree.statement.batch.element.DimTableElement;
import com.aliyun.fastmodel.core.tree.statement.batch.element.FromTableElement;
import com.aliyun.fastmodel.core.tree.statement.batch.element.IndicatorDefine;
import com.aliyun.fastmodel.core.tree.statement.batch.element.TableList;
import com.aliyun.fastmodel.core.tree.statement.batch.element.TimePeriodElement;
import com.aliyun.fastmodel.core.tree.statement.businessprocess.CreateBusinessProcess;
import com.aliyun.fastmodel.core.tree.statement.businessprocess.RenameBusinessProcess;
import com.aliyun.fastmodel.core.tree.statement.businessprocess.SetBusinessProcessComment;
import com.aliyun.fastmodel.core.tree.statement.businessprocess.SetBusinessProcessProperties;
import com.aliyun.fastmodel.core.tree.statement.businessprocess.UnSetBusinessProcessProperties;
import com.aliyun.fastmodel.core.tree.statement.businessunit.CreateBusinessUnit;
import com.aliyun.fastmodel.core.tree.statement.businessunit.SetBusinessUnitComment;
import com.aliyun.fastmodel.core.tree.statement.command.ExportSql;
import com.aliyun.fastmodel.core.tree.statement.command.HelpCommand;
import com.aliyun.fastmodel.core.tree.statement.command.ImportSql;
import com.aliyun.fastmodel.core.tree.statement.delete.Delete;
import com.aliyun.fastmodel.core.tree.statement.desc.Describe;
import com.aliyun.fastmodel.core.tree.statement.dict.CreateDict;
import com.aliyun.fastmodel.core.tree.statement.dict.DropDict;
import com.aliyun.fastmodel.core.tree.statement.dict.RenameDict;
import com.aliyun.fastmodel.core.tree.statement.dict.SetDictComment;
import com.aliyun.fastmodel.core.tree.statement.dict.SetDictProperties;
import com.aliyun.fastmodel.core.tree.statement.dimension.AddDimensionAttribute;
import com.aliyun.fastmodel.core.tree.statement.dimension.ChangeDimensionAttribute;
import com.aliyun.fastmodel.core.tree.statement.dimension.CreateDimension;
import com.aliyun.fastmodel.core.tree.statement.dimension.DropDimensionAttribute;
import com.aliyun.fastmodel.core.tree.statement.dimension.attribute.DimensionAttribute;
import com.aliyun.fastmodel.core.tree.statement.domain.CreateDomain;
import com.aliyun.fastmodel.core.tree.statement.domain.RenameDomain;
import com.aliyun.fastmodel.core.tree.statement.domain.SetDomainComment;
import com.aliyun.fastmodel.core.tree.statement.domain.SetDomainProperties;
import com.aliyun.fastmodel.core.tree.statement.domain.UnSetDomainProperties;
import com.aliyun.fastmodel.core.tree.statement.dqc.AddDqcRule;
import com.aliyun.fastmodel.core.tree.statement.dqc.ChangeDqcRule;
import com.aliyun.fastmodel.core.tree.statement.dqc.ChangeDqcRuleElement;
import com.aliyun.fastmodel.core.tree.statement.dqc.CreateDqcRule;
import com.aliyun.fastmodel.core.tree.statement.dqc.DropDqcRule;
import com.aliyun.fastmodel.core.tree.statement.dqc.check.BaseCheckElement;
import com.aliyun.fastmodel.core.tree.statement.dqc.check.TableCheckElement;
import com.aliyun.fastmodel.core.tree.statement.element.MultiComment;
import com.aliyun.fastmodel.core.tree.statement.group.CreateGroup;
import com.aliyun.fastmodel.core.tree.statement.group.DropGroup;
import com.aliyun.fastmodel.core.tree.statement.group.SetGroupComment;
import com.aliyun.fastmodel.core.tree.statement.group.SetGroupProperties;
import com.aliyun.fastmodel.core.tree.statement.indicator.CreateAtomicCompositeIndicator;
import com.aliyun.fastmodel.core.tree.statement.indicator.CreateAtomicIndicator;
import com.aliyun.fastmodel.core.tree.statement.indicator.CreateDerivativeCompositeIndicator;
import com.aliyun.fastmodel.core.tree.statement.indicator.CreateDerivativeIndicator;
import com.aliyun.fastmodel.core.tree.statement.indicator.CreateIndicator;
import com.aliyun.fastmodel.core.tree.statement.indicator.DropIndicator;
import com.aliyun.fastmodel.core.tree.statement.indicator.RenameIndicator;
import com.aliyun.fastmodel.core.tree.statement.indicator.SetIndicatorComment;
import com.aliyun.fastmodel.core.tree.statement.indicator.SetIndicatorProperties;
import com.aliyun.fastmodel.core.tree.statement.insert.Insert;
import com.aliyun.fastmodel.core.tree.statement.layer.AddChecker;
import com.aliyun.fastmodel.core.tree.statement.layer.Checker;
import com.aliyun.fastmodel.core.tree.statement.layer.CreateLayer;
import com.aliyun.fastmodel.core.tree.statement.layer.DropChecker;
import com.aliyun.fastmodel.core.tree.statement.layer.DropLayer;
import com.aliyun.fastmodel.core.tree.statement.layer.RenameLayer;
import com.aliyun.fastmodel.core.tree.statement.layer.SetLayerComment;
import com.aliyun.fastmodel.core.tree.statement.layer.SetLayerProperties;
import com.aliyun.fastmodel.core.tree.statement.materialize.CreateMaterialize;
import com.aliyun.fastmodel.core.tree.statement.materialize.DropMaterialize;
import com.aliyun.fastmodel.core.tree.statement.materialize.RenameMaterialize;
import com.aliyun.fastmodel.core.tree.statement.materialize.SetMaterializeAlias;
import com.aliyun.fastmodel.core.tree.statement.materialize.SetMaterializeComment;
import com.aliyun.fastmodel.core.tree.statement.materialize.SetMaterializeRefProperties;
import com.aliyun.fastmodel.core.tree.statement.measure.unit.CreateMeasureUnit;
import com.aliyun.fastmodel.core.tree.statement.measure.unit.DropMeasureUnit;
import com.aliyun.fastmodel.core.tree.statement.measure.unit.RenameMeasureUnit;
import com.aliyun.fastmodel.core.tree.statement.measure.unit.SetMeasureUnitComment;
import com.aliyun.fastmodel.core.tree.statement.measure.unit.SetMeasureUnitProperties;
import com.aliyun.fastmodel.core.tree.statement.pipe.CreatePipe;
import com.aliyun.fastmodel.core.tree.statement.references.MoveReferences;
import com.aliyun.fastmodel.core.tree.statement.references.ShowReferences;
import com.aliyun.fastmodel.core.tree.statement.rule.AddRules;
import com.aliyun.fastmodel.core.tree.statement.rule.ChangeRuleElement;
import com.aliyun.fastmodel.core.tree.statement.rule.ChangeRules;
import com.aliyun.fastmodel.core.tree.statement.rule.CreateRules;
import com.aliyun.fastmodel.core.tree.statement.rule.DropRule;
import com.aliyun.fastmodel.core.tree.statement.rule.RuleDefinition;
import com.aliyun.fastmodel.core.tree.statement.rule.RuleStrategy;
import com.aliyun.fastmodel.core.tree.statement.rule.function.BaseFunction;
import com.aliyun.fastmodel.core.tree.statement.rule.function.column.ColumnFunction;
import com.aliyun.fastmodel.core.tree.statement.rule.function.table.TableFunction;
import com.aliyun.fastmodel.core.tree.statement.rule.strategy.DynamicStrategy;
import com.aliyun.fastmodel.core.tree.statement.rule.strategy.FixedStrategy;
import com.aliyun.fastmodel.core.tree.statement.rule.strategy.VolInterval;
import com.aliyun.fastmodel.core.tree.statement.rule.strategy.VolStrategy;
import com.aliyun.fastmodel.core.tree.statement.script.ImportObject;
import com.aliyun.fastmodel.core.tree.statement.script.RefRelation;
import com.aliyun.fastmodel.core.tree.statement.show.ShowObjects;
import com.aliyun.fastmodel.core.tree.statement.show.ShowSingleStatistic;
import com.aliyun.fastmodel.core.tree.statement.show.ShowStatistic;
import com.aliyun.fastmodel.core.tree.statement.showcreate.Output;
import com.aliyun.fastmodel.core.tree.statement.showcreate.ShowCreate;
import com.aliyun.fastmodel.core.tree.statement.table.AddCols;
import com.aliyun.fastmodel.core.tree.statement.table.AddConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.AddPartitionCol;
import com.aliyun.fastmodel.core.tree.statement.table.ChangeCol;
import com.aliyun.fastmodel.core.tree.statement.table.CloneTable;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateCodeTable;
import com.aliyun.fastmodel.core.tree.statement.table.CreateIndex;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.DropCol;
import com.aliyun.fastmodel.core.tree.statement.table.DropConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.DropIndex;
import com.aliyun.fastmodel.core.tree.statement.table.DropPartitionCol;
import com.aliyun.fastmodel.core.tree.statement.table.DropTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.RenameCol;
import com.aliyun.fastmodel.core.tree.statement.table.RenameTable;
import com.aliyun.fastmodel.core.tree.statement.table.SetColComment;
import com.aliyun.fastmodel.core.tree.statement.table.SetColProperties;
import com.aliyun.fastmodel.core.tree.statement.table.SetColumnOrder;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableAliasedName;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableComment;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableProperties;
import com.aliyun.fastmodel.core.tree.statement.table.TableElement;
import com.aliyun.fastmodel.core.tree.statement.table.UnSetColProperties;
import com.aliyun.fastmodel.core.tree.statement.table.UnSetTableProperties;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.ColumnGroupConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.DimConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.LevelConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.LevelDefine;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.NotNullConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.RedundantConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.TimePeriodConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.UniqueConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.index.IndexColumnName;
import com.aliyun.fastmodel.core.tree.statement.table.index.TableIndex;
import com.aliyun.fastmodel.core.tree.statement.timeperiod.CreateTimePeriod;
import com.aliyun.fastmodel.core.tree.statement.timeperiod.DropTimePeriod;
import com.aliyun.fastmodel.core.tree.statement.timeperiod.RenameTimePeriod;
import com.aliyun.fastmodel.core.tree.statement.timeperiod.SetTimePeriodComment;
import com.aliyun.fastmodel.core.tree.statement.timeperiod.SetTimePeriodProperties;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/10/29
 */
public abstract class AstVisitor<R, C> implements IAstVisitor<R, C> {

    /**
     * visit statement
     *
     * @param statement 语句
     * @param context   上下文
     * @return R
     */
    @Override
    public R visitStatement(BaseStatement statement, C context) {
        return visitNode(statement, context);
    }

    /**
     * visit insert
     *
     * @param insert  Insert expr
     * @param context context
     * @return R
     */
    public R visitInsert(Insert insert, C context) {
        return visitStatement(insert, context);
    }

    /**
     * visit createTable
     *
     * @param createTable 创建表
     * @param context     上下文
     * @return R
     */
    public R visitCreateTable(CreateTable createTable, C context) {
        return visitStatement(createTable, context);
    }

    /**
     * visit constraints
     *
     * @param baseConstraint
     * @param context
     * @return
     */
    public R visitConstraint(BaseConstraint baseConstraint, C context) {
        return visitTableElement(baseConstraint, context);
    }

    /**
     * visit Table Element
     *
     * @param tableElement tableElement
     * @param context      context
     * @return R
     */
    public R visitTableElement(TableElement tableElement, C context) {
        return visitNode(tableElement, context);
    }

    /**
     * visit primary Constraint
     *
     * @param primaryConstraint 主要的constraint
     * @param context           上下文
     * @return R
     */
    public R visitPrimaryConstraint(PrimaryConstraint primaryConstraint, C context) {
        return visitConstraint(primaryConstraint, context);
    }

    /**
     * visit level define
     *
     * @param levelDefine level define
     * @param context     context
     * @return R
     */
    public R visitLevelDefine(LevelDefine levelDefine, C context) {
        return visitNode(levelDefine, context);
    }

    /**
     * visit level constraint
     *
     * @param levelConstraint level constraint
     * @param context         context
     * @return R
     */
    public R visitLevelConstraint(LevelConstraint levelConstraint, C context) {
        return visitConstraint(levelConstraint, context);
    }

    /**
     * visit col Type
     *
     * @param columnDefine colType
     * @param context      context
     * @return R
     */
    public R visitColumnDefine(ColumnDefinition columnDefine, C context) {
        return visitTableElement(columnDefine, context);
    }

    /**
     * visit rename col
     *
     * @param renameCol rename Col
     * @param context   context
     * @return R
     */
    public R visitChangeCol(ChangeCol renameCol, C context) {
        return visitStatement(renameCol, context);
    }

    /**
     * visit unSet Table Properties
     *
     * @param unSetTableProperties unset table Properties
     * @param context              context
     * @return R
     */
    public R visitUnSetTableProperties(UnSetTableProperties unSetTableProperties, C context) {
        return visitBaseUnSetProperties(unSetTableProperties, context);
    }

    /**
     * visit set TableProperties
     *
     * @param setTableProperties set Table Properties
     * @param context            context
     * @return R
     */
    public R visitSetTableProperties(SetTableProperties setTableProperties, C context) {
        return visitBaseSetProperties(setTableProperties, context);
    }

    /**
     * rename comment
     *
     * @param setTableComment comment
     * @param context         context
     * @return R
     */
    public R visitSetTableComment(SetTableComment setTableComment, C context) {
        return visitBaseSetComment(setTableComment, context);
    }

    /**
     * visit add constraint
     *
     * @param addConstraint addConstraint
     * @param context       context
     * @return R
     */
    public R visitAddConstraint(AddConstraint addConstraint, C context) {
        return visitStatement(addConstraint, context);
    }

    /**
     * visit add constraint
     *
     * @param renameIndicatorComment addConstraint
     * @param context                context
     * @return R
     */
    public R visitSetIndicatorComment(SetIndicatorComment renameIndicatorComment,
                                      C context) {
        return visitBaseSetComment(renameIndicatorComment, context);
    }

    /**
     * visit add constraint
     *
     * @param renameIndicatorName addConstraint
     * @param context             context
     * @return R
     */

    public R visitRenameIndicator(RenameIndicator renameIndicatorName, C context) {
        return visitBaseRename(renameIndicatorName, context);
    }

    /**
     * visit set properties
     *
     * @param setIndicatorProperties addConstraint
     * @param context                context
     * @return R
     */
    public R visitSetIndicatorProperties(SetIndicatorProperties setIndicatorProperties, C context) {
        return visitBaseSetProperties(setIndicatorProperties, context);
    }

    /**
     * visit create Indicator
     *
     * @param createIndicator 创建指标
     * @param context         context
     * @return R
     */
    public R visitCreateIndicator(CreateIndicator createIndicator, C context) {
        return visitBaseCreate(createIndicator, context);
    }

    /**
     * visit set bp Comment
     *
     * @param setBpComment setBpComment
     * @param context      context
     * @return R
     */
    public R visitSetBpComment(SetBusinessProcessComment setBpComment, C context) {
        return visitBaseSetComment(setBpComment, context);
    }

    /**
     * set BuComment
     *
     * @param setBuComment setComment
     * @param context      context
     * @return R
     */
    public R visitSetBuComment(SetBusinessUnitComment setBuComment, C context) {
        return visitBaseSetComment(setBuComment, context);
    }

    /**
     * visit add Col
     *
     * @param addCols add Col
     * @param context context
     * @return R
     */
    public R visitAddCols(AddCols addCols, C context) {
        return visitStatement(addCols, context);
    }

    /**
     * visit drop Constraint
     *
     * @param dropConstraint dropConstraint
     * @param context        context
     * @return R
     */
    public R visitDropConstraint(DropConstraint dropConstraint, C context) {
        return visitStatement(dropConstraint, context);
    }

    /**
     * visit rename materialize
     *
     * @param renameMaterialize materialize
     * @param context           context
     * @return R
     */
    public R visitRenameMaterialize(RenameMaterialize renameMaterialize, C context) {
        return visitStatement(renameMaterialize, context);
    }

    /**
     * visit set materialize ref properties
     *
     * @param setMaterializeRefProperties properties
     * @param context                     context
     * @return R
     */
    public R visitSetMaterializeRefProperties(
        SetMaterializeRefProperties setMaterializeRefProperties, C context) {
        return visitStatement(setMaterializeRefProperties, context);
    }

    /**
     * visit materialize comment
     *
     * @param setMaterializeComment comment
     * @param context               context
     * @return R
     */
    public R visitSetMaterializeComment(SetMaterializeComment setMaterializeComment,
                                        C context) {
        return visitBaseSetComment(setMaterializeComment, context);
    }

    /**
     * visit set domain properties
     *
     * @param setDomainProperties properties
     * @param context             context
     * @return R
     */
    public R visitSetDomainProperties(SetDomainProperties setDomainProperties, C context) {
        return visitBaseSetProperties(setDomainProperties, context);
    }

    /**
     * visit rename bp
     *
     * @param renameBp rename bp
     * @param context  context
     * @return R
     */
    public R visitRenameBp(RenameBusinessProcess renameBp, C context) {
        return visitBaseRename(renameBp, context);
    }

    /**
     * un set bp properties
     *
     * @param unSetBpProperties unset properties
     * @param context           context
     * @return R
     */
    public R visitUnSetBpProperties(UnSetBusinessProcessProperties unSetBpProperties, C context) {
        return visitBaseUnSetProperties(unSetBpProperties, context);
    }

    /**
     * visit set bp properties
     *
     * @param setBpProperties set bp properties
     * @param context         context
     * @return R
     */
    public R visitSetBpProperties(SetBusinessProcessProperties setBpProperties, C context) {
        return visitBaseSetProperties(setBpProperties, context);
    }

    /**
     * visit create domain
     *
     * @param createDomain 创建领域
     * @param context      context
     * @return R
     */
    public R visitCreateDomain(CreateDomain createDomain, C context) {
        return visitBaseCreate(createDomain, context);
    }

    /**
     * visit set comment
     *
     * @param baseSetComment set Comment
     * @param context        context
     * @return R
     */
    public R visitBaseSetComment(BaseSetComment baseSetComment, C context) {
        return visitStatement(baseSetComment, context);
    }

    public R visitBaseUnSetProperties(BaseUnSetProperties baseUnSetProperties,
                                      C context) {
        return visitStatement(baseUnSetProperties, context);
    }

    public R visitUnSetDomain(UnSetDomainProperties unSetDomainProperties,
                              C context) {
        return visitBaseUnSetProperties(unSetDomainProperties, context);
    }

    /**
     * visit not Null constraint
     *
     * @param notNullConstraint notNull
     * @param context           context
     * @return R
     */
    public R visitNotNullConstraint(
        NotNullConstraint notNullConstraint, C context) {
        return visitConstraint(notNullConstraint, context);
    }

    public R visitRenameTable(RenameTable renameTable, C context) {
        return visitBaseOperatorStatement(renameTable, context);
    }

    public R visitBaseOperatorStatement(BaseOperatorStatement baseOperatorStatement, C context) {
        return visitStatement(baseOperatorStatement, context);
    }

    public R visitBaseCreate(BaseCreate baseCreate, C context) {
        return visitBaseOperatorStatement(baseCreate, context);
    }

    public R visitBaseRename(BaseRename baseRename, C context) {
        return visitBaseOperatorStatement(baseRename, context);
    }

    public R visitDropTable(DropTable dropTable, C context) {
        return visitBaseOperatorStatement(dropTable, context);
    }

    public R visitCreateMaterialize(CreateMaterialize createMaterialize,
                                    C context) {
        return visitBaseCreate(createMaterialize, context);
    }

    public R visitDropMaterialize(DropMaterialize dropMaterialize, C context) {
        return visitBaseDrop(dropMaterialize, context);
    }

    public R visitBaseDrop(BaseDrop baseDrop, C context) {
        return visitBaseOperatorStatement(baseDrop, context);
    }

    public R visitCreateBp(CreateBusinessProcess createBp, C context) {
        return visitBaseCreate(createBp, context);
    }

    public R visitCreateBu(CreateBusinessUnit createBu, C context) {
        return visitBaseCreate(createBu, context);
    }

    public R createAtomicCompositeIndicator(
        CreateAtomicCompositeIndicator createAtomicCompositeIndicator, C context) {
        return visitCreateIndicator(createAtomicCompositeIndicator, context);
    }

    public R visitCreateAtomicIndicator(CreateAtomicIndicator createAtomicIndicator,
                                        C context) {
        return visitCreateIndicator(createAtomicIndicator, context);
    }

    public R visitCreateDerivativeCompositeIndicator(
        CreateDerivativeCompositeIndicator createDerivativeCompositeIndicator, C context) {
        return visitCreateIndicator(createDerivativeCompositeIndicator, context);
    }

    public R visitCreateDerivativeIndicator(
        CreateDerivativeIndicator createDerivativeIndicator, C context) {
        return visitCreateIndicator(createDerivativeIndicator, context);
    }

    public R visitDropIndicator(DropIndicator dropIndicator, C context) {
        return visitBaseDrop(dropIndicator, context);
    }

    /**
     * 创建修饰词
     *
     * @param createAdjunct 创建修饰词
     * @param context       context
     * @return R
     */
    public R visitCreateAdjunct(CreateAdjunct createAdjunct, C context) {
        return visitBaseCreate(createAdjunct, context);
    }

    public R visitRenameAdjunct(RenameAdjunct renameAdjunct, C context) {
        return visitBaseRename(renameAdjunct, context);
    }

    public R visitSetAdjunctComment(SetAdjunctComment setAdjunctComment, C context) {
        return visitBaseSetComment(setAdjunctComment, context);
    }

    public R visitDropAdjunct(DropAdjunct dropAdjunct, C context) {
        return visitBaseDrop(dropAdjunct, context);
    }

    public R visitCreateTimePeriod(CreateTimePeriod createTimePeriod, C context) {
        return visitBaseCreate(createTimePeriod, context);
    }

    public R visitRenameTimePeriod(RenameTimePeriod renameTimePeriod, C context) {
        return visitBaseRename(renameTimePeriod, context);
    }

    public R visitSetTimePeriodComment(SetTimePeriodComment setTimePeriodComment,
                                       C context) {
        return visitBaseSetComment(setTimePeriodComment, context);
    }

    public R visitSetTimePeriodExpression(
        SetTimePeriodProperties setTimePeriodProperties, C context) {
        return visitBaseOperatorStatement(setTimePeriodProperties, context);
    }

    public R visitDropTimePeriod(DropTimePeriod dropTimePeriod, C context) {
        return visitBaseDrop(dropTimePeriod, context);
    }

    public R visitCreateMeasureUnit(CreateMeasureUnit createMeasureUnit,
                                    C context) {
        return visitBaseCreate(createMeasureUnit, context);
    }

    public R visitSetMeasureUnitComment(
        SetMeasureUnitComment setMeasureUnitComment, C context) {
        return visitBaseSetComment(setMeasureUnitComment, context);
    }

    public R visitSetMeasureUnitProperties(
        SetMeasureUnitProperties setMeasureUnitProperties, C context) {
        return visitBaseSetProperties(setMeasureUnitProperties, context);
    }

    public R visitBaseSetProperties(BaseSetProperties baseSetProperties, C context) {
        return visitBaseOperatorStatement(baseSetProperties, context);
    }

    public R visitRenameMeasureUnit(RenameMeasureUnit renameMeasureUnit,
                                    C context) {
        return visitBaseRename(renameMeasureUnit, context);
    }

    public R visitDropMeasureUnit(DropMeasureUnit dropMeasureUnit, C context) {
        return visitBaseDrop(dropMeasureUnit, context);
    }

    public R visitCreateDict(CreateDict createDict, C context) {
        return visitBaseCreate(createDict, context);
    }

    public R visitRenameDict(RenameDict renameDict, C context) {
        return visitBaseRename(renameDict, context);
    }

    public R visitSetDictProperties(SetDictProperties setDictProperties, C context) {
        return visitBaseSetProperties(setDictProperties, context);
    }

    public R visitDropDict(DropDict dropDict, C context) {
        return visitBaseDrop(dropDict, context);
    }

    public R setAdjunctProperties(SetAdjunctProperties setAdjunctProperties, C context) {
        return visitBaseSetProperties(setAdjunctProperties, context);
    }

    public R visitSetDictComment(SetDictComment setDictComment, C context) {
        return visitBaseSetComment(setDictComment, context);
    }

    public R visitDimConstraint(DimConstraint dimConstraint, C context) {
        return visitConstraint(dimConstraint, context);
    }

    public R visitDescribe(Describe describe, C context) {
        return visitBaseQueryStatement(describe, context);
    }

    public R visitCreateLayer(CreateLayer createLayer, C context) {
        return visitBaseCreate(createLayer, context);
    }

    public R visitDropLayer(DropLayer dropLayer, C context) {
        return visitBaseDrop(dropLayer, context);
    }

    public R visitRenameLayer(RenameLayer renameLayer, C context) {
        return visitBaseRename(renameLayer, context);
    }

    public R visitSetLayerComment(SetLayerComment setLayerComment, C context) {
        return visitBaseSetComment(setLayerComment, context);
    }

    public R visitSetLayerProperties(SetLayerProperties setLayerProperties, C context) {
        return visitBaseSetProperties(setLayerProperties, context);
    }

    public R visitAddChecker(AddChecker addChecker, C context) {
        return visitBaseOperatorStatement(addChecker, context);
    }

    public R visitDropChecker(DropChecker dropChecker, C context) {
        return visitBaseOperatorStatement(dropChecker, context);
    }

    /**
     * show create statement
     *
     * @param showCreate showCreate
     * @param context    context
     * @return R
     */
    public R visitShowCreate(ShowCreate showCreate, C context) {
        return visitBaseQueryStatement(showCreate, context);
    }

    /**
     * show statement
     *
     * @param showObjects 显示的对象
     * @param context     上下文
     * @return R
     */
    public R visitShowObjects(ShowObjects showObjects, C context) {
        return visitBaseQueryStatement(showObjects, context);
    }

    public R visitChecker(Checker checker, C context) {
        return visitNode(checker, context);
    }

    public R visitPartitionedBy(PartitionedBy partitionedBy, C context) {
        return visitNode(partitionedBy, context);
    }

    public R visitColumnGroupConstraint(
        ColumnGroupConstraint columnGroupConstraint, C context) {
        return visitConstraint(columnGroupConstraint, context);
    }

    public R visitBaseQueryStatement(BaseQueryStatement baseQueryStatement, C context) {
        return visitStatement(baseQueryStatement, context);
    }

    public R visitCreateGroup(CreateGroup createGroup, C context) {
        return visitBaseCreate(createGroup, context);
    }

    public R visitSetGroupComment(SetGroupComment setGroupComment, C context) {
        return visitBaseSetComment(setGroupComment, context);
    }

    public R visitSetGroupProperties(SetGroupProperties setGroupProperties, C context) {
        return visitBaseSetProperties(setGroupProperties, context);
    }

    public R visitDropGroup(DropGroup dropGroup, C context) {
        return visitBaseDrop(dropGroup, context);
    }

    public R visitCreateCodeTable(CreateCodeTable createCodeTable, C context) {
        return visitCreateTable(createCodeTable, context);
    }

    public R visitDelete(Delete delete, C context) {
        return visitBaseOperatorStatement(delete, context);
    }

    public R visitOutput(Output output, C context) {
        return visitNode(output, context);
    }

    public R visitDropCol(DropCol dropCol, C context) {
        return visitBaseOperatorStatement(dropCol, context);
    }

    public R visitDropPartitionCol(DropPartitionCol dropPartitionCol, C context) {
        return visitBaseOperatorStatement(dropPartitionCol, context);
    }

    public R visitAddPartitionCol(AddPartitionCol addPartitionCol, C context) {
        return visitBaseOperatorStatement(addPartitionCol, context);
    }

    public R visitDateField(DateField dateField, C context) {
        return visitNode(dateField, context);
    }

    public R visitIndicatorDefine(IndicatorDefine indicatorDefine, C context) {
        return visitBatchElement(indicatorDefine, context);
    }

    public R visitCreateIndicatorBatch(CreateIndicatorBatch createBatch,
                                       C context) {
        return visitBaseOperatorStatement(createBatch, context);
    }

    public R visitBatchElement(AbstractBatchElement batchElement, C context) {
        return visitNode(batchElement, context);
    }

    public R visitTimePeriodElement(TimePeriodElement timePeriodElement, C context) {
        return visitBatchElement(timePeriodElement, context);
    }

    public R visitFromTableElement(FromTableElement fromTableElement, C context) {
        return visitBatchElement(fromTableElement, context);
    }

    public R visitDimTableElement(DimTableElement dimTableElement, C context) {
        return visitBatchElement(dimTableElement, context);
    }

    public R visitTableList(TableList tableList, C context) {
        return visitNode(tableList, context);
    }

    public R visitDimPathElement(DimPathElement dimPathElement, C context) {
        return visitBatchElement(dimPathElement, context);
    }

    public R visitDefaultAdjunct(DefaultAdjunct defaultAdjunct, C context) {
        return visitBatchElement(defaultAdjunct, context);
    }

    public R visitRenameCol(RenameCol renameCol, C context) {
        return visitBaseOperatorStatement(renameCol, context);
    }

    public R visitSetColComment(SetColComment setColComment, C context) {
        return visitBaseSetComment(setColComment, context);
    }

    public R visitSetColProperties(SetColProperties setColProperties, C context) {
        return visitBaseSetProperties(setColProperties, context);
    }

    public R visitUnSetColProperties(UnSetColProperties unSetColProperties, C context) {
        return visitBaseUnSetProperties(unSetColProperties, context);
    }

    public R visitSetDomainComment(SetDomainComment setDomainComment, C context) {
        return visitBaseSetComment(setDomainComment, context);
    }

    public R visitRenameDomain(RenameDomain renameDomain, C context) {
        return visitBaseRename(renameDomain, context);
    }

    public R visitCreatePipe(CreatePipe createPipe, C context) {
        return visitBaseCreate(createPipe, context);
    }

    public R visitMultiComment(MultiComment multiComment, C context) {
        return visitNode(multiComment, context);
    }

    public R visitBaseSetAliasedName(BaseSetAliasedName baseSetAliasedName, C context) {
        return visitBaseOperatorStatement(baseSetAliasedName, context);
    }

    public R visitSetTableAliasedName(SetTableAliasedName setTableAliasedName, C context) {
        return visitBaseSetAliasedName(setTableAliasedName, context);
    }

    public R visitFixedStrategy(FixedStrategy fixedStrategy, C context) {
        return visitRuleStrategy(fixedStrategy, context);
    }

    public R visitRuleStrategy(RuleStrategy ruleStrategy, C context) {
        return visitNode(ruleStrategy, context);
    }

    public R visitVolStrategy(VolStrategy volStrategy, C context) {
        return visitRuleStrategy(volStrategy, context);
    }

    public R visitDynamicStrategy(DynamicStrategy dynamicStrategy, C context) {
        return visitRuleStrategy(dynamicStrategy, context);
    }

    public R visitBaseFunction(BaseFunction baseFunction, C context) {
        FunctionCall functionCall = new FunctionCall(
            QualifiedName.of(baseFunction.funcName().name()),
            false, baseFunction.arguments()
        );
        return visitFunctionCall(functionCall, context);
    }

    public R visitColumnFunction(ColumnFunction columnFunction,
                                 C context) {
        return visitBaseFunction(columnFunction, context);
    }

    public R visitTableFunction(TableFunction tableFunction,
                                C context) {
        return visitBaseFunction(tableFunction, context);
    }

    public R visitDropRule(DropRule dropRule, C context) {
        return visitBaseOperatorStatement(dropRule, context);
    }

    public R visitCreateRules(CreateRules createRules, C context) {
        return visitBaseCreate(createRules, context);
    }

    public R visitAddRules(AddRules addRules, C context) {
        return visitBaseOperatorStatement(addRules, context);
    }

    public R visitChangeRules(ChangeRules changeRules, C context) {
        return visitBaseOperatorStatement(changeRules, context);
    }

    public R visitRuleDefinition(RuleDefinition ruleDefinition, C context) {
        return visitNode(ruleDefinition, context);
    }

    public R visitBaseCheckElement(BaseCheckElement baseCheckElement, C context) {
        return visitNode(baseCheckElement, context);
    }

    public R visitCreateDqcRule(CreateDqcRule createDqcRule, C context) {
        return visitBaseCreate(createDqcRule, context);
    }

    public R visitChangeRuleElement(ChangeRuleElement changeRuleElement, C context) {
        return visitNode(changeRuleElement, context);
    }

    public R visitSetMaterializeAlias(SetMaterializeAlias setMaterializeAlias,
                                      C context) {
        return visitBaseSetAliasedName(setMaterializeAlias, context);
    }

    public R visitTimePeriodConstraint(
        TimePeriodConstraint timePeriodConstraint, C context) {
        return visitConstraint(timePeriodConstraint, context);
    }

    public R visitVolInterval(VolInterval volInterval, C context) {
        return visitExpression(volInterval, context);
    }

    public R visitTableCheckElement(TableCheckElement tableCheckElement, C context) {
        return visitBaseCheckElement(tableCheckElement, context);
    }

    public R visitAddDqcRule(AddDqcRule addDqcRule, C context) {
        return visitBaseOperatorStatement(addDqcRule, context);
    }

    public R visitChangeDqcRule(ChangeDqcRule changeDqcRule, C context) {
        return visitBaseOperatorStatement(changeDqcRule, context);
    }

    public R visitDropDqcRule(DropDqcRule dropDqcRule, C context) {
        return visitBaseOperatorStatement(dropDqcRule, context);
    }

    public R visitChangeDqcRuleElement(ChangeDqcRuleElement changeDqcRuleElement, C context) {
        return visitNode(changeDqcRuleElement, context);
    }

    public R visitUniqueConstraint(UniqueConstraint uniqueConstraint,
                                   C context) {
        return visitConstraint(uniqueConstraint, context);
    }

    public R visitRedundantConstraint(
        RedundantConstraint redundantConstraint, C context) {
        return visitConstraint(redundantConstraint, context);
    }

    public R visitCloneTable(CloneTable cloneTable, C context) {
        return visitBaseCreate(cloneTable, context);
    }

    public R visitIndexColumnName(IndexColumnName indexColumnName,
                                  C context) {
        return visitNode(indexColumnName, context);
    }

    public R visitTableIndex(TableIndex tableIndex, C context) {
        return visitTableElement(tableIndex, context);
    }

    public R visitDropIndex(DropIndex dropIndex, C context) {
        return visitBaseDrop(dropIndex, context);
    }

    public R visitCreateIndex(CreateIndex createIndex, C context) {
        return visitBaseOperatorStatement(createIndex, context);
    }

    public R visitImportEntityStatement(ImportObject importEntityStatement,
                                        C context) {
        return visitBaseOperatorStatement(importEntityStatement, context);
    }

    public R visitRefEntityStatement(RefRelation refEntityStatement, C context) {
        return visitBaseOperatorStatement(refEntityStatement, context);
    }

    public R visitChangeDimensionField(ChangeDimensionAttribute changeDimensionField,
                                       C context) {
        return visitBaseOperatorStatement(changeDimensionField, context);
    }

    public R visitCreateDimension(CreateDimension createDimension, C context) {
        return visitBaseCreate(createDimension, context);
    }

    public R visitAddDimensionField(AddDimensionAttribute addDimensionField, C context) {
        return visitBaseOperatorStatement(addDimensionField, context);
    }

    public R visitDropDimensionField(DropDimensionAttribute dropDimensionField,
                                     C context) {
        return visitBaseOperatorStatement(dropDimensionField, context);
    }

    public R visitDimensionField(DimensionAttribute dimensionField, C context) {
        return visitNode(dimensionField, context);
    }

    public R visitSetColumnOrder(SetColumnOrder setColumnOrder, C context) {
        return visitBaseOperatorStatement(setColumnOrder, context);
    }

    public R visitMoveReferences(MoveReferences moveReferences, C context) {
        return visitBaseOperatorStatement(moveReferences, context);
    }

    public R visitBaseCommandStatement(BaseCommandStatement baseCommandStatement, C context) {
        return visitStatement(baseCommandStatement, context);
    }

    public R visitImportSql(ImportSql importSql, C context) {
        return visitBaseCommandStatement(importSql, context);
    }

    public R visitExportSql(ExportSql exportSql, C context) {
        return visitBaseCommandStatement(exportSql, context);
    }

    public R visitHelpCommand(HelpCommand helpCommand, C context) {
        return visitBaseCommandStatement(helpCommand, context);
    }

    public R visitShowReferences(ShowReferences showReferences, C context) {
        return visitBaseQueryStatement(showReferences, context);
    }

    public R visitShowSingleStatistic(ShowSingleStatistic showSingleStatistic, C context) {
        return visitBaseQueryStatement(showSingleStatistic, context);
    }

    public R visitShowStatistic(ShowStatistic showStatistic, C context) {
        return visitBaseQueryStatement(showStatistic, context);
    }
}

