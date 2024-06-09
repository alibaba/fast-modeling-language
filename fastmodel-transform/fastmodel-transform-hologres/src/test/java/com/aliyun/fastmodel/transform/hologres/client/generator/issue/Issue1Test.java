/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hologres.client.generator.issue;

import java.util.List;

import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.client.dto.request.DdlReverseSqlRequest;
import com.aliyun.fastmodel.transform.api.client.dto.result.DdlTableResult;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.client.generator.DefaultCodeGenerator;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Issue test
 *
 * @author panguanjing
 * @date 2022/6/28
 */
public class Issue1Test {

    @Test
    public void testIssue() {
        DefaultCodeGenerator defaultCodeGenerator = new DefaultCodeGenerator();
        DdlReverseSqlRequest request = DdlReverseSqlRequest
            .builder()
            .code("BEGIN;\n"
                + "CREATE TABLE molin_db.molin_db.aa_not_exist_1 (\n"
                + "   id                         BIGINT NOT NULL,\n"
                + "   name                       TEXT NOT NULL,\n"
                + "   aa_not_exist_1             TEXT,\n"
                + "   _data_integration_deleted_ BOOLEAN NOT NULL\n"
                + ");\n"
                + "CALL SET_TABLE_PROPERTY('molin_db.molin_db.aa_not_exist_1', 'time_to_live_in_seconds', '2592000');\n"
                + "CALL SET_TABLE_PROPERTY('molin_db.molin_db.aa_not_exist_1', 'orientation', 'row');\n"
                + "CALL SET_TABLE_PROPERTY('molin_db.molin_db.aa_not_exist_1', 'binlog.level', 'none');\n"
                + "COMMENT ON COLUMN molin_db.molin_db.aa_not_exist_1.id IS '';\n"
                + "COMMENT ON COLUMN molin_db.molin_db.aa_not_exist_1.name IS '';\n"
                + "COMMENT ON COLUMN molin_db.molin_db.aa_not_exist_1.aa_not_exist_1 IS '';\n"
                + "COMMENT ON COLUMN molin_db.molin_db.aa_not_exist_1._data_integration_deleted_ IS 'Auto generated logical delete column';\n"
                + "COMMIT;")
            .dialectMeta(DialectMeta.DEFAULT_HOLO)
            .build();
        DdlTableResult reverse = defaultCodeGenerator.reverse(request);
        assertEquals(reverse.getTable().getDatabase(), "molin_db");
        assertEquals(reverse.getTable().getSchema(), "molin_db");
        assertEquals(reverse.getTable().getName(), "aa_not_exist_1");
        assertEquals(reverse.getTable().getColumns().size(), 4);
        Column column = reverse.getTable().getColumns().get(3);
        assertEquals(column.getComment(), "Auto generated logical delete column");
        List<BaseClientProperty> properties = reverse.getTable().getProperties();
        assertEquals(2, properties.size());
        assertEquals(reverse.getTable().getLifecycleSeconds(), new Long(2592000L));
    }
}
