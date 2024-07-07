package com.aliyun.fastmodel.transform.oceanbase.client;

import java.util.List;

import com.aliyun.fastmodel.transform.api.client.CodeGenerator;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.client.dto.request.DdlGeneratorModelRequest;
import com.aliyun.fastmodel.transform.api.client.dto.result.DdlGeneratorResult;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.client.dto.table.TableConfig;
import com.aliyun.fastmodel.transform.api.client.generator.DefaultCodeGenerator;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.extension.client.property.table.TablePartitionRaw;
import com.aliyun.fastmodel.transform.oceanbase.BaseOceanbaseTest;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * oceanbase generator test
 *
 * @author panguanjing
 * @date 2024/2/5
 */
public class OceanBaseGeneratorWithModelTest extends BaseOceanbaseTest {

    private CodeGenerator codeGenerator = new DefaultCodeGenerator();

    @Test
    public void testGenerator() {
        List<Column> columns = Lists.newArrayList();
        Column e = Column.builder()
            .name("c1")
            .dataType("int")
            .comment("column_comment")
            .nullable(false)
            .build();
        columns.add(e);
        List<BaseClientProperty> properties = Lists.newArrayList();
        TablePartitionRaw tablePartitionRaw = new TablePartitionRaw();
        tablePartitionRaw.setValueString("PARTITION BY RANGE COLUMNS(col1)\n"
            + "        (PARTITION p0 VALUES LESS THAN(100),\n"
            + "         PARTITION p1 VALUES LESS THAN(200),\n"
            + "         PARTITION p2 VALUES LESS THAN(300)\n"
            + "        );");
        properties.add(tablePartitionRaw);
        Table table = Table.builder()
            .name("t1")
            .comment("comment")
            .columns(columns)
            .properties(properties)
            .build();
        assertText("CREATE TABLE IF NOT EXISTS t1 \n"
            + "(\n"
            + "   c1 INT NOT NULL COMMENT 'column_comment'\n"
            + ")\n"
            + "COMMENT 'comment'\n"
            + "PARTITION BY RANGE COLUMNS(col1)\n"
            + "        (PARTITION p0 VALUES LESS THAN(100),\n"
            + "         PARTITION p1 VALUES LESS THAN(200),\n"
            + "         PARTITION p2 VALUES LESS THAN(300)\n"
            + "        );", table);
    }

    private void assertText(String expect, Table table) {
        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .config(TableConfig.builder()
                .dialectMeta(DialectMeta.DEFAULT_OB_MYSQL)
                .build())
            .after(table)
            .build();
        DdlGeneratorResult generate = codeGenerator.generate(request);
        assertEquals(expect, generate.getDialectNodes().get(0).getNode());
    }
}
