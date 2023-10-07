package com.aliyun.fastmodel.transform.hive.format;

import java.util.List;

import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2023/2/15
 */
public class HiveHelperTest {

    @Test
    public void testAppendLocation() {
        List<Property> properties = Lists.newArrayList();
        properties.add(new Property(HivePropertyKey.LOCATION.getValue(), "hdfs://abc"));
        CreateTable node = CreateTable.builder()
            .tableName(QualifiedName.of("abc"))
            .properties(properties).build();
        String s = HiveHelper.appendLocation(node);
        assertEquals(s, "LOCATION 'hdfs://abc'");
    }
}