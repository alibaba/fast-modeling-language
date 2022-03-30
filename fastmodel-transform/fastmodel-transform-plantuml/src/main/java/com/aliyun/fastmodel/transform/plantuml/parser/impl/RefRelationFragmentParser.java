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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.script.RefDirection;
import com.aliyun.fastmodel.core.tree.statement.script.RefObject;
import com.aliyun.fastmodel.core.tree.statement.script.RefRelation;
import com.aliyun.fastmodel.core.tree.util.IdentifierUtil;
import com.aliyun.fastmodel.transform.api.domain.table.ConstraintDataModel;
import com.aliyun.fastmodel.transform.api.domain.table.ConstraintDataModel.ConstraintDataModelBuilder;
import com.aliyun.fastmodel.transform.api.domain.table.TableColModel;
import com.aliyun.fastmodel.transform.plantuml.exception.VisualParseException;
import com.aliyun.fastmodel.transform.plantuml.parser.Fragment;
import com.aliyun.fastmodel.transform.plantuml.parser.FragmentImpl;
import com.aliyun.fastmodel.transform.plantuml.parser.FragmentParser;
import com.aliyun.fastmodel.transform.template.BaseFmlTemplateFactory;
import com.aliyun.fastmodel.transform.template.FmlTemplate;
import org.apache.commons.collections.CollectionUtils;

/**
 * 关系的调用处理
 *
 * @author panguanjing
 * @date 2021/10/3
 */
public class RefRelationFragmentParser implements FragmentParser {

    private final FmlTemplate template;

    public RefRelationFragmentParser() {
        template = BaseFmlTemplateFactory.getInstance().getTemplate("fragment/refRelation.ftl");
    }

    @Override
    public Fragment parse(BaseStatement statement) throws VisualParseException {
        RefRelation relation = (RefRelation)statement;
        FragmentImpl fragment = new FragmentImpl();
        RefObject left = relation.getLeft();
        RefObject right = relation.getRight();
        List<Identifier> columnList = left.getAttrNameList();
        List<ConstraintDataModel> list = new ArrayList<>();
        String code = getArrow(relation.getRefDirection());
        if (CollectionUtils.isNotEmpty(columnList)) {
            list = IntStream.range(0, columnList.size())
                .mapToObj(
                    index -> {
                        Identifier identifier = columnList.get(index);
                        return getConstraintDataModel(relation, identifier, index);
                    }
                ).collect(Collectors.toList());
        } else {
            ConstraintDataModelBuilder
                constraintDataModelBuilder = getConstraintDataModelBuilder(relation);
            list.add(constraintDataModelBuilder.build());
        }
        Map<String, Object> root = new HashMap<>(1);
        root.put("constraints", list);
        fragment.setContent(template.process(root));
        return fragment;
    }

    private ConstraintDataModelBuilder getConstraintDataModelBuilder(RefRelation relation) {

        TableColModel tableColModel = new TableColModel();
        RefObject left = relation.getLeft();
        tableColModel.setTable(left.getMainName().getSuffix());
        TableColModel rightTableCol = new TableColModel();
        rightTableCol.setTable(relation.getRight().getMainName().getSuffix());
        String identifier = relation.getIdentifier();

        ConstraintDataModelBuilder constraintDataModelBuilder = ConstraintDataModel.builder()
            .left(tableColModel)
            .right(rightTableCol)
            .direction(getArrow(
                relation.getRefDirection()))
            .leftComment(left.getCommentValue())
            .rightComment(relation.getRight()
                .getCommentValue());
        if (!IdentifierUtil.isSysIdentifier(new Identifier(identifier))) {
            constraintDataModelBuilder.constraintName(identifier);
        }
        return constraintDataModelBuilder;
    }

    private ConstraintDataModel getConstraintDataModel(RefRelation relation, Identifier identifier, int index) {
        TableColModel leftColModel = new TableColModel();
        RefObject left = relation.getLeft();
        leftColModel.setTable(left.getMainName().getSuffix());
        leftColModel.setColumn(identifier.getValue());
        RefObject right = relation.getRight();
        Identifier rightColumn = right.getAttrNameList().get(index);
        TableColModel rightTableCol = new TableColModel();
        rightTableCol.setTable(right.getMainName().getSuffix());
        rightTableCol.setColumn(rightColumn.getValue());

        //setting
        String relName = relation.getIdentifier();
        if (IdentifierUtil.isSysIdentifier(new Identifier(relName))) {
            relName = null;
        }
        return ConstraintDataModel.builder()
            .left(leftColModel)
            .right(rightTableCol)
            .leftComment(left.getCommentValue())
            .rightComment(right.getCommentValue())
            .direction(getArrow(relation.getRefDirection()))
            .constraintName(relName)
            .build();
    }

    private String getArrow(RefDirection refDirection) {
        switch (refDirection) {
            case LEFT_DIRECTION_RIGHT:
                return "->";
            case RIGHT_DIRECTON_LEFT:
                return "<-";
            case LEFT_VERTICAL_DIRECTION_RIGHT:
                return "-->";
            case RIGHT_VERTICAL_DIRECTON_LEFT:
                return "<--";
            default:
                return "-->";
        }
    }
}
