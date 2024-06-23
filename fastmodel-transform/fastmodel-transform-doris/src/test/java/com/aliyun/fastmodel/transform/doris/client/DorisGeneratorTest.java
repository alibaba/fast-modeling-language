package com.aliyun.fastmodel.transform.doris.client;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import com.aliyun.fastmodel.transform.api.client.dto.constraint.Constraint;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.client.dto.property.StringProperty;
import com.aliyun.fastmodel.transform.api.client.dto.request.DdlGeneratorModelRequest;
import com.aliyun.fastmodel.transform.api.client.dto.request.DdlReverseSqlRequest;
import com.aliyun.fastmodel.transform.api.client.dto.result.DdlGeneratorResult;
import com.aliyun.fastmodel.transform.api.client.dto.result.DdlTableResult;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.client.dto.table.TableConfig;
import com.aliyun.fastmodel.transform.api.client.generator.DefaultCodeGenerator;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.api.extension.client.constraint.ClientConstraintType;
import com.google.common.collect.Lists;
import lombok.SneakyThrows;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * doris generator test
 *
 * @author panguanjing
 * @date 2024/1/21
 */
public class DorisGeneratorTest {

    DefaultCodeGenerator defaultCodeGenerator = new DefaultCodeGenerator();

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
        Table table = Table.builder()
            .name("t1")
            .comment("comment")
            .columns(columns)
            .build();
        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .config(TableConfig.builder()
                .dialectMeta(DialectMeta.DEFAULT_DORIS)
                .build())
            .after(table)
            .build();
        DdlGeneratorResult generate = defaultCodeGenerator.generate(request);
        assertEquals("CREATE TABLE IF NOT EXISTS t1\n"
            + "(\n"
            + "   c1 INT NOT NULL COMMENT \"column_comment\"\n"
            + ")\n"
            + "COMMENT \"comment\";", generate.getDialectNodes().get(0).getNode());
    }

    @Test
    public void testGeneratorArrayInt() {
        List<Column> columns = Lists.newArrayList();
        Column e = Column.builder()
            .name("c1")
            .dataType("array<int>")
            .comment("column_comment")
            .nullable(false)
            .build();
        columns.add(e);
        Table table = Table.builder()
            .name("t1")
            .comment("comment")
            .columns(columns)
            .build();
        DdlGeneratorModelRequest request = DdlGeneratorModelRequest.builder()
            .config(TableConfig.builder()
                .dialectMeta(DialectMeta.DEFAULT_DORIS)
                .build())
            .after(table)
            .build();
        DdlGeneratorResult generate = defaultCodeGenerator.generate(request);
        assertEquals("CREATE TABLE IF NOT EXISTS t1\n"
            + "(\n"
            + "   c1 ARRAY<INT> NOT NULL COMMENT \"column_comment\"\n"
            + ")\n"
            + "COMMENT \"comment\";", generate.getDialectNodes().get(0).getNode());
    }

    @Test
    public void testUniqueKey() {
        List<Column> columns = Lists.newArrayList();
        DdlGeneratorModelRequest request = new DdlGeneratorModelRequest();
        columns.add(Column.builder()
            .dataType("tinyint")
            .name("c1")
            .nullable(false)
            .comment("ti_comment")
            .build());
        columns.add(Column.builder()
            .dataType("smallint")
            .name("c2")
            .nullable(false)
            .primaryKey(false)
            .comment("si_comment")
            .build());
        List<Constraint> constraints = Lists.newArrayList();
        ArrayList<String> strings = Lists.newArrayList("c1", "c2");
        Constraint constraint = Constraint.builder().columns(strings)
            .type(ClientConstraintType.UNIQUE_KEY)
            .build();
        constraints.add(constraint);
        Table after = Table.builder()
            .name("abc")
            .columns(columns)
            .comment("comment")
            .constraints(constraints)
            .build();
        request.setAfter(after);
        DdlGeneratorResult generate = getDdlGeneratorResult(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        assertEquals(1, dialectNodes.size());
        assertEquals("CREATE TABLE IF NOT EXISTS abc\n"
            + "(\n"
            + "   c1 TINYINT NOT NULL COMMENT \"ti_comment\",\n"
            + "   c2 SMALLINT NOT NULL COMMENT \"si_comment\"\n"
            + ")\n"
            + "UNIQUE KEY (c1,c2)\n"
            + "COMMENT \"comment\";", dialectNodes.get(0).getNode());
    }

    @Test
    public void testGeneratorWithProperty() {
        DdlGeneratorModelRequest request = new DdlGeneratorModelRequest();
        List<Column> columns = Lists.newArrayList();
        columns.add(Column.builder()
            .dataType("tinyint")
            .name("c1")
            .nullable(false)
            .comment("ti_comment")
            .build());
        List<BaseClientProperty> properties = Lists.newArrayList();
        StringProperty stringProperty = new StringProperty();
        stringProperty.setKey("replication_allocation");
        stringProperty.setValueString("tag.location.default: 1");
        properties.add(stringProperty);
        Table after = Table.builder()
            .name("abc")
            .columns(columns)
            .comment("comment")
            .properties(properties)
            .build();
        request.setAfter(after);
        DdlGeneratorResult generate = getDdlGeneratorResult(request);
        List<DialectNode> dialectNodes = generate.getDialectNodes();
        assertEquals("CREATE TABLE IF NOT EXISTS abc\n"
            + "(\n"
            + "   c1 TINYINT NOT NULL COMMENT \"ti_comment\"\n"
            + ")\n"
            + "COMMENT \"comment\"\n"
            + "PROPERTIES (\"replication_allocation\"=\"tag.location.default: 1\");", dialectNodes.get(0).getNode());
    }

    @Test
    @SneakyThrows
    public void testReverse() {
        String content = IOUtils.resourceToString("/doris/range_error.txt", Charset.defaultCharset());
        DdlReverseSqlRequest request = DdlReverseSqlRequest.builder()
            .code(content)
            .dialectMeta(DialectMeta.DEFAULT_DORIS)
            .build();
        DdlTableResult reverse = defaultCodeGenerator.reverse(request);
        assertEquals("fml_raw_partition", reverse.getTable().getName());
    }

    private DdlGeneratorResult getDdlGeneratorResult(DdlGeneratorModelRequest request) {
        request.setConfig(TableConfig.builder()
            .dialectMeta(DialectMeta.DEFAULT_DORIS)
            .build());
        return defaultCodeGenerator.generate(request);
    }
}
