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

import java.util.List;

import com.aliyun.fastmodel.core.parser.DomainLanguage;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.script.RefDirection;
import com.aliyun.fastmodel.core.tree.statement.script.RefObject;
import com.aliyun.fastmodel.core.tree.statement.script.RefRelation;
import com.aliyun.fastmodel.parser.NodeParser;
import com.aliyun.fastmodel.transform.plantuml.parser.Fragment;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/10/3
 */
public class RefRelationFragmentParserTest {

    RefRelationFragmentParser refRelationFragmentParser = new RefRelationFragmentParser();

    NodeParser nodeParser = new NodeParser();

    @Test
    public void testParserWithVeritcal() {
        String fml = "REF a >-> b;";
        BaseStatement parse = nodeParser.parse(new DomainLanguage(fml));
        Fragment parse1 = refRelationFragmentParser.parse(parse);
        String content = parse1.content();
        assertEquals(content, "    a --> b \n");
    }

    @Test
    public void testParser() {
        RefRelation relation = new RefRelation(
            QualifiedName.of("abc")
            , new RefObject(QualifiedName.of("t"), null, new Comment("comment"))
            , new RefObject(QualifiedName.of("t1"), null, new Comment("comment"))
            , RefDirection.LEFT_DIRECTION_RIGHT
        );
        Fragment parse = refRelationFragmentParser.parse(relation);
        String content = parse.content();
        assertEquals(content, "    t \"comment\"  -> \"comment\"  t1 :abc\n");
    }

    @Test
    public void testParserWithColumn() {
        List<Identifier> c1 = ImmutableList.of(new Identifier("c1"), new Identifier("c3"));
        List<Identifier> c2 = ImmutableList.of(new Identifier("c2"), new Identifier("c4"));
        RefRelation relation = new RefRelation(
            QualifiedName.of("abc")
            , new RefObject(QualifiedName.of("t"), c1, new Comment("comment"))
            , new RefObject(QualifiedName.of("t1"), c2, new Comment("comment"))
            , RefDirection.LEFT_DIRECTION_RIGHT
        );
        Fragment parse = refRelationFragmentParser.parse(relation);
        String content = parse.content();
        assertEquals(content, "    t::c1 \"comment\"  -> \"comment\"  t1::c2 :abc\n"
            + "    t::c3 \"comment\"  -> \"comment\"  t1::c4 :abc\n");
    }
}