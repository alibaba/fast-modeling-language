package com.aliyun.fastmodel.transform.doris.format;

import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.desc.DistributeConstraint;
import com.aliyun.fastmodel.transform.doris.context.DorisContext;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2024/1/21
 */
public class DorisOutVisitorTest {

    DorisOutVisitor dorisOutVisitor = new DorisOutVisitor(DorisContext.builder().build());

    @Test
    public void testVisitDorisGenericDataType() {
        DistributeConstraint distributeConstraint1 = new DistributeConstraint(Lists.newArrayList(
            new Identifier("c1"),
            new Identifier("c2")
        ), 10);
        dorisOutVisitor.visitDistributeKeyConstraint(distributeConstraint1, 0);
        String string = dorisOutVisitor.getBuilder().toString();
        assertEquals("DISTRIBUTED BY HASH(c1,c2) BUCKETS 10", string);
    }

    @Test
    public void testVisitCreateTable() {

    }
}