/*
 * Copyright 2020 Alibaba Group Holding Ltd.
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

package com.aliyun.fastmodel.transform.mc;

import java.util.ArrayList;
import java.util.List;

import com.aliyun.fastmodel.core.tree.BaseRelation;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.ComparisonExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.atom.TableOrColumn;
import com.aliyun.fastmodel.core.tree.expr.enums.ComparisonOperator;
import com.aliyun.fastmodel.core.tree.relation.AliasedRelation;
import com.aliyun.fastmodel.core.tree.relation.Join;
import com.aliyun.fastmodel.core.tree.relation.join.JoinCriteria;
import com.aliyun.fastmodel.core.tree.relation.join.JoinOn;
import com.aliyun.fastmodel.core.tree.relation.join.JoinToken;
import com.aliyun.fastmodel.core.tree.relation.querybody.Table;
import com.aliyun.fastmodel.core.tree.util.QueryUtil;
import com.aliyun.fastmodel.transform.mc.context.MaxComputeContext;
import com.aliyun.fastmodel.transform.mc.format.MaxComputeFormatter;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author panguanjing
 * @date 2020/11/17
 */
public class JoinTest {

    @Test
    public void testFlatRelation() {
        AliasedRelation aliasedRelation = new AliasedRelation(new Table(QualifiedName.of("junit.dwd_fact_order")),
            new Identifier("dwd_fct_order"));
        List<RelationDim> list = init();
        BaseRelation baseRelation = toFlatRelation(aliasedRelation, list);
        String s = MaxComputeFormatter.formatSql(baseRelation, MaxComputeContext.builder().build()).getNode();
        assertEquals(s, "junit.dwd_fact_order dwd_fct_order\n"
            + "LEFT JOIN t0 a0 ON t0.f0 = r0.f0\n"
            + "LEFT JOIN t1 a1 ON t1.f1 = r1.f1\n"
            + "LEFT JOIN t2 a2 ON t2.f2 = r2.f2\n"
            + "LEFT JOIN t3 a3 ON t3.f3 = r3.f3\n"
            + "LEFT JOIN t4 a4 ON t4.f4 = r4.f4");
    }

    private List<RelationDim> init() {
        List<RelationDim> relationDims = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            RelationDim relationDim = new RelationDim(
                QueryUtil.aliased(
                    new Table(QualifiedName.of("t" + i)),
                    "a" + i,
                    ImmutableList.of()
                ), new JoinOn(
                new ComparisonExpression(
                    ComparisonOperator.EQUAL,
                    new TableOrColumn(QualifiedName.of("t" + i, "f" + i)),
                    new TableOrColumn(QualifiedName.of("r" + i, "f" + i))
                )
            ));

            relationDims.add(relationDim);
        }
        return relationDims;
    }

    private BaseRelation toFlatRelation(AliasedRelation aliasedRelation, List<RelationDim> list) {
        if (list.isEmpty()) {
            return aliasedRelation;
        }
        RelationDim remove = list.remove(list.size() - 1);
        return new Join(JoinToken.LEFT, toFlatRelation(aliasedRelation, list), remove.baseRelation,
            remove.joinCriteria);
    }

    public static class RelationDim {
        /**
         * relation
         */
        private final BaseRelation baseRelation;
        /**
         * join criteria
         */
        private final JoinCriteria joinCriteria;

        public RelationDim(BaseRelation baseRelation, JoinCriteria joinCriteria) {
            this.baseRelation = baseRelation;
            this.joinCriteria = joinCriteria;
        }

        public BaseRelation getBaseRelation() {
            return baseRelation;
        }

        public JoinCriteria getJoinCriteria() {
            return joinCriteria;
        }
    }

}