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

package com.aliyun.fastmodel.core.semantic.table;

import java.util.HashSet;
import java.util.Set;

import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.LevelConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.LevelDefine;
import org.junit.Test;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/11/20
 */
public class LevelConstraintSemanticCheckTest {

    @Test
    public void check() {
        Set<String> set = new HashSet<>();
        set.add("abc");
        LevelConstraintSemanticCheck check = new LevelConstraintSemanticCheck(
            set
        );
        java.util.List<LevelDefine> levelDefines = new java.util.ArrayList<>();
        levelDefines.add(new LevelDefine(new Identifier("abc"), null));
        check.check(new LevelConstraint(new Identifier("1234"),
            levelDefines,
            new Comment("xxx")
        ));
    }
}