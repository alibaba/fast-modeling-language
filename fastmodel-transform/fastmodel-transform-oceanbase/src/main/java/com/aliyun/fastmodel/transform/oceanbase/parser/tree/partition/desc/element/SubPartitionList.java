package com.aliyun.fastmodel.transform.oceanbase.parser.tree.partition.desc.element;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AbstractNode;
import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.transform.oceanbase.parser.visitor.OceanBaseMysqlAstVisitor;
import lombok.Getter;

/**
 * SubPartitionList
 *
 * @author panguanjing
 * @date 2024/2/6
 */
@Getter
public class SubPartitionList extends AbstractNode {

    private final List<SubPartitionElement> subPartitionElementList;

    public SubPartitionList(List<SubPartitionElement> subPartitionElementList) {this.subPartitionElementList = subPartitionElementList;}

    @Override
    public List<? extends Node> getChildren() {
        return subPartitionElementList;
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        OceanBaseMysqlAstVisitor<R, C> astVisitor = (OceanBaseMysqlAstVisitor<R, C>)visitor;
        return astVisitor.visitSubPartitionList(this, context);
    }
}
