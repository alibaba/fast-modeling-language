/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hologres.client.generator.issue;

import com.aliyun.fastmodel.transform.api.client.dto.request.DdlReverseSqlRequest;
import com.aliyun.fastmodel.transform.api.client.dto.result.DdlTableResult;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.client.generator.DefaultCodeGenerator;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2022/6/29
 */
public class Issue2Test {

    @Test
    public void testGenerator() {
        DefaultCodeGenerator defaultCodeGenerator = new DefaultCodeGenerator();
        DdlReverseSqlRequest request = DdlReverseSqlRequest
            .builder()
            .code("BEGIN;\n"
                + "/*\n"
                + "DROP TABLE molin_db.aa_exist_1;\n"
                + "*/\n"
                + "\n"
                + "-- Type: TABLE ; Name: aa_exist_1; Owner: molin_db_developer\n"
                + "\n"
                + "CREATE TABLE molin_db.aa_exist_1 (\n"
                + "    id bigint NOT NULL,\n"
                + "    name text,\n"
                + "    aa_exist_1 text\n"
                + "    ,PRIMARY KEY (id)\n"
                + ");\n"
                + "CALL set_table_property('molin_db.aa_exist_1', 'distribution_key', 'id');\n"
                + "CALL set_table_property('molin_db.aa_exist_1', 'table_group', 'molin_db_tg_default');\n"
                + "CALL set_table_property('molin_db.aa_exist_1', 'orientation', 'column');\n"
                + "CALL set_table_property('molin_db.aa_exist_1', 'storage_format', 'orc');\n"
                + "CALL set_table_property('molin_db.aa_exist_1', 'time_to_live_in_seconds', '3153600000');\n"
                + "CALL set_table_property('molin_db.aa_exist_1', 'dictionary_encoding_columns', 'name:auto,aa_exist_1:auto');\n"
                + "CALL set_table_property('molin_db.aa_exist_1', 'bitmap_columns', 'name,aa_exist_1');\n"
                + "\n"
                + "END;")
            .dialectMeta(DialectMeta.DEFAULT_HOLO)
            .build();
        DdlTableResult reverse = defaultCodeGenerator.reverse(request);
        Table table = reverse.getTable();
        assertEquals(table.getName(), "aa_exist_1");
        assertEquals(table.getProperties().size(), 6);
        assertEquals(table.getColumns().size(), 3);
        assertEquals(table.getConstraints().size(), 1);
        assertEquals(table.getLifecycleSeconds(), new Long(3153600000L));
    }
}
