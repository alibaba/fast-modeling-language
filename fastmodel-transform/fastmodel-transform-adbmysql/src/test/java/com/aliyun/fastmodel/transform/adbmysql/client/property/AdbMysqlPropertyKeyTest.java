package com.aliyun.fastmodel.transform.adbmysql.client.property;

import com.aliyun.fastmodel.transform.adbmysql.format.AdbMysqlPropertyKey;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2023/2/12
 */
public class AdbMysqlPropertyKeyTest {

    @Test
    public void getByValue() {
        AdbMysqlPropertyKey byValue = AdbMysqlPropertyKey.getByValue("ADB_MYSQL.partition_date_format");
        assertNotNull(byValue);
    }

    @Test
    public void testGetNull() {
        AdbMysqlPropertyKey propertyKey = AdbMysqlPropertyKey.getByValue("not");
        assertNull(propertyKey);
    }
}