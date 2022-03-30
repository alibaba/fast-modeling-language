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

package com.aliyun.fastmodel.compare.diff;

import java.util.List;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.constants.TableDetailType;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.github.difflib.DiffUtils;
import com.github.difflib.algorithm.DiffAlgorithmListener;
import com.github.difflib.patch.AbstractDelta;
import com.github.difflib.patch.Patch;
import com.google.common.collect.Lists;
import org.junit.Test;

/**
 * DiffUtils test
 *
 * @author panguanjing
 * @date 2021/11/5
 */
public class DiffUtilsTest {

    List<BaseStatement> before = Lists.newArrayList(
        CreateTable.builder()
            .tableName(QualifiedName.of("c"))
            .detailType(TableDetailType.NORMAL_DIM)
            .build(),
        CreateTable.builder()
            .tableName(QualifiedName.of("b"))
            .detailType(TableDetailType.NORMAL_DIM)
            .build(),
        CreateTable.builder()
            .tableName(QualifiedName.of("a"))
            .detailType(TableDetailType.NORMAL_DIM)
            .build()
    );

    List<BaseStatement> after = Lists.newArrayList(
        CreateTable.builder()
            .tableName(QualifiedName.of("b"))
            .detailType(TableDetailType.NORMAL_DIM).build(),
        CreateTable.builder()
            .tableName(QualifiedName.of("c"))
            .detailType(TableDetailType.NORMAL_DIM).build(),
        CreateTable.builder()
            .tableName(QualifiedName.of("x"))
            .detailType(TableDetailType.NORMAL_DIM).build()
    );

    @Test
    public void testDiffDemo() {
        Patch<String> diff = DiffUtils.diff(Lists.newArrayList("a", "b", "c", "d", "e"),
            Lists.newArrayList("b", "a", "d", "e"));
        diff.getDeltas();
    }

    @Test
    public void testDiff() {
        Patch<String> diff = DiffUtils.diff("ABCABBA", "CBABAC", new DiffAlgorithmListener() {
            @Override
            public void diffStart() {

            }

            @Override
            public void diffStep(int value, int max) {

            }

            @Override
            public void diffEnd() {

            }
        });
        System.out.println(diff.getDeltas().get(0).getTarget());
    }

    private void extracted(List<String> left, List<String> right) {
        Patch<String> diff = DiffUtils.diff(left, right);
        for (AbstractDelta<String> delta : diff.getDeltas()) {
            System.out.println(delta + "" + delta.getType());
        }

    }

    @Test
    public void testDiff2() {
        List<String> left = Lists.newArrayList(
            "create table b (a bigint comment 'comment')",
            "create table b(b bigint comment 'comment')"
        );
        List<String> right = Lists.newArrayList(
            "create table b (a bigint comment 'comment')",
            "create table a(b bigint comment 'comment')"
        );
        extracted(left, right);
    }
}
