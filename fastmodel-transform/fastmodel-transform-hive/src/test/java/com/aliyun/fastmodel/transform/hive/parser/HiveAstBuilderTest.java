package com.aliyun.fastmodel.transform.hive.parser;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.constants.TableDetailType;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.parser.NodeParser;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.hive.HiveTransformer;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

import java.util.List;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;

/**
 * @author 子梁
 * @date 2023/11/10
 */
public class HiveAstBuilderTest {
    HiveTransformer hiveTransformer = new HiveTransformer();

    NodeParser nodeParser = new NodeParser();

    @Test
    public void testReverse() {
        String code = "CREATE TABLE `default.test_ziliang_v1`(\n" +
                "  `a` string COMMENT 'aa')\n" +
                "COMMENT '123'\n" +
                "PARTITIONED BY ( \n" +
                "  `ds` string COMMENT '')\n" +
                "ROW FORMAT SERDE \n" +
                "  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' \n" +
                "STORED AS INPUTFORMAT \n" +
                "  'org.apache.hadoop.mapred.TextInputFormat' \n" +
                "OUTPUTFORMAT \n" +
                "  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'\n" +
                "LOCATION\n" +
                "  'hdfs://master-1-1.c-aac6b952af7280c8.cn-shanghai.emr.aliyuncs.com:9000/user/hive/warehouse/test_ziliang_v1'\n" +
                "TBLPROPERTIES (\n" +
                "  'bucketing_version'='2', \n" +
                "  'transient_lastDdlTime'='1698806349')";
        BaseStatement reverse = hiveTransformer.reverse(new DialectNode(code), ReverseContext.builder().build());
        MatcherAssert.assertThat(reverse.toString(), Matchers.equalTo("CREATE DIM TABLE `default.test_ziliang_v1` \n" +
                "(\n" +
                "   `a` STRING COMMENT 'aa'\n" +
                ")\n" +
                "COMMENT '123'\n" +
                "PARTITIONED BY\n" +
                "(\n" +
                "   `ds` STRING COMMENT ''\n" +
                ")\n" +
                "WITH('bucketing_version'='2','transient_lastDdlTime'='1698806349'," +
                "'hive.row_format_serde'='org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'," +
                "'hive.stored_input_format'='org.apache.hadoop.mapred.TextInputFormat'," +
                "'hive.stored_output_format'='org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'," +
                "'hive.location'='hdfs://master-1-1.c-aac6b952af7280c8.cn-shanghai.emr.aliyuncs.com:9000/user/hive/warehouse/test_ziliang_v1')"));
    }
}