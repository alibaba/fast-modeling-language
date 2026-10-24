package com.aliyun.fastmodel.transform.mc;

import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.SetColumnOrder;
import com.aliyun.fastmodel.core.tree.util.DataTypeUtil;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Desc: MaxCompute 支持 CHANGE COLUMN ... AFTER 调整列顺序、但没有 FIRST 关键字，故 SetColumnOrder
 * 按能力如实声明可执行性：AFTER 定位可执行、正常下发；first=true(移到首位)不可执行、被发布链路
 * DialectNode#isExecutable 过滤，从而不下发引擎、发布前 fastsql 校验也不会撞 FIRST。
 *
 * @author 莫泣
 * @date 2026-06-29
 */
public class MaxComputeSetColumnOrderExecutableTest {

    private final MaxComputeTransformer transformer = new MaxComputeTransformer();

    @Test
    public void firstColumnOrderShouldNotBeExecutable() {
        SetColumnOrder setColumnOrder = new SetColumnOrder(
            QualifiedName.of("t"),
            new Identifier("scene"),
            new Identifier("scene"),
            DataTypeUtil.simpleType(DataTypeEnums.STRING),
            null,
            true);
        DialectNode node = transformer.transform(setColumnOrder, TransformContext.builder().build());
        assertFalse(node.isExecutable());
    }

    @Test
    public void afterColumnOrderShouldBeExecutable() {
        SetColumnOrder setColumnOrder = new SetColumnOrder(
            QualifiedName.of("t"),
            new Identifier("scene"),
            new Identifier("scene"),
            DataTypeUtil.simpleType(DataTypeEnums.STRING),
            new Identifier("other"),
            false);
        DialectNode node = transformer.transform(setColumnOrder, TransformContext.builder().build());
        assertTrue(node.isExecutable());
        assertTrue(node.getNode().contains("AFTER"));
    }
}
