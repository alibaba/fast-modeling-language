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

package com.aliyun.fastmodel.transform.example.dialect;

import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.api.dialect.transform.DialectTransform;
import com.aliyun.fastmodel.transform.api.dialect.transform.DialectTransformParam;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Dialect Transform test
 *
 * @author panguanjing
 * @date 2021/7/26
 */
public class DialectTransformTest {

    @Test
    public void testTransformOracleToMysql() {
        DialectTransformParam param = DialectTransformParam.builder()
            .sourceMeta(DialectMeta.getByName(DialectName.ORACLE))
            .sourceNode(new DialectNode("CREATE TABLE a (b BIGINT);"))
            .targetMeta(DialectMeta.DEFAULT_MYSQL)
            .build();
        DialectNode transform = DialectTransform.transform(param);
        String node = transform.getNode();
        assertEquals(node, "CREATE TABLE a\n"
            + "(\n"
            + "   b BIGINT\n"
            + ")"
        );
    }

    @Test
    public void testTransformOracelToMysqlMerge() {
        DialectTransformParam param = DialectTransformParam.builder()
            .sourceMeta(DialectMeta.getByName(DialectName.ORACLE))
            .sourceNode(new DialectNode("CREATE TABLE a (b BIGINT); COMMENT ON TABLE a IS  'comment';"))
            .targetMeta(DialectMeta.DEFAULT_MYSQL)
            .build();
        DialectNode transform = DialectTransform.transform(param);
        assertEquals(transform.getNode(), "CREATE TABLE a\n"
            + "(\n"
            + "   b BIGINT\n"
            + ") COMMENT 'comment'"
        );
    }

    @Test
    public void testTransformOracelToHiveMerge() {
        DialectTransformParam param = DialectTransformParam.builder()
            .sourceMeta(DialectMeta.getByName(DialectName.ORACLE))
            .sourceNode(new DialectNode("CREATE TABLE a (b BIGINT); COMMENT ON TABLE a IS  'comment';"))
            .targetMeta(DialectMeta.DEFAULT_HIVE)
            .build();
        DialectNode transform = DialectTransform.transform(param);
        assertEquals(transform.getNode(), "CREATE TABLE a\n"
            + "(\n"
            + "   b BIGINT\n"
            + ")\n"
            + "COMMENT 'comment'"
        );
    }

    @Test
    public void testMultiFile() {
        DialectNode sourceNode = new DialectNode(
            "COMMENT ON TABLE dim_shop IS 'comment';\n COMMENT ON TABLE dim_shop IS 'comment';");
        DialectTransformParam param = DialectTransformParam.builder()
            .sourceMeta(DialectMeta.getByName(DialectName.ORACLE))
            .sourceNode(sourceNode)
            .targetMeta(DialectMeta.getByName(DialectName.FML))
            .build();
        DialectNode dialectNode = DialectTransform.transform(param);
        assertEquals(dialectNode.getNode(), "ALTER TABLE dim_shop SET COMMENT 'comment';\n"
            + "ALTER TABLE dim_shop SET COMMENT 'comment';");
    }

    @Test
    public void testFmlToOracleMultiFile() {
        DialectNode sourceNode = new DialectNode(
            "ALTER TABLE dim_shop SET COMMENT 'comment';\n ALTER TABLE dim_shop SET COMMENT 'comment';");
        DialectTransformParam param = DialectTransformParam.builder()
            .sourceMeta(DialectMeta.getByName(DialectName.FML))
            .sourceNode(sourceNode)
            .targetMeta(DialectMeta.getByName(DialectName.ORACLE))
            .build();
        DialectNode dialectNode = DialectTransform.transform(param);
        assertEquals(dialectNode.getNode(), "COMMENT ON TABLE dim_shop IS 'comment';\n"
            + "COMMENT ON TABLE dim_shop IS 'comment';");
    }

    @Test
    public void testOracleToFml() {
        DialectNode sourceNode = new DialectNode(
            "CREATE TABLE dim_shop(a VARCHAR2(10)); "
        );
        DialectTransformParam param = DialectTransformParam.builder()
            .sourceMeta(DialectMeta.getByName(DialectName.ORACLE))
            .sourceNode(sourceNode)
            .targetMeta(DialectMeta.getByName(DialectName.FML))
            .build();
        DialectNode dialectNode = DialectTransform.transform(param);
        assertEquals(dialectNode.getNode(), "CREATE DIM TABLE dim_shop \n"
            + "(\n"
            + "   a VARCHAR(10)\n"
            + ")");
    }

    @Test
    public void testOracleToFmlChar() {
        DialectNode sourceNode = new DialectNode(
            "CREATE TABLE dim_shop(a CHAR(10)); "
        );
        DialectTransformParam param = DialectTransformParam.builder()
            .sourceMeta(DialectMeta.getByName(DialectName.ORACLE))
            .sourceNode(sourceNode)
            .targetMeta(DialectMeta.getByName(DialectName.FML))
            .build();
        DialectNode dialectNode = DialectTransform.transform(param);
        assertEquals(dialectNode.getNode(), "CREATE DIM TABLE dim_shop \n"
            + "(\n"
            + "   a CHAR(10)\n"
            + ")");
    }

    @Test
    public void testOracleToMysql() {
        DialectNode source = new DialectNode(
            "create table dim_shop(a LONG);"
        );
        DialectTransformParam param = DialectTransformParam.builder()
            .sourceMeta(DialectMeta.getByName(DialectName.ORACLE))
            .sourceNode(source)
            .targetMeta(DialectMeta.getByName(DialectName.MYSQL))
            .build();
        DialectNode transform = DialectTransform.transform(param);
        assertEquals("CREATE TABLE dim_shop\n"
            + "(\n"
            + "   a LONGTEXT\n"
            + ")", transform.getNode());
    }
}
