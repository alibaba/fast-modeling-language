package com.aliyun.fastmodel.transform.starrocks.client.constraint;

import com.aliyun.fastmodel.transform.api.extension.client.constraint.ClientConstraintType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2023/12/13
 */
public class ClientConstraintTypeTest {

    @Test
    public void testGetByValue() {
        ClientConstraintType primaryKey = ClientConstraintType.getByValue("primary");
        assertEquals(primaryKey, ClientConstraintType.PRIMARY_KEY);

        ClientConstraintType unique = ClientConstraintType.getByValue("unique");
        assertEquals(unique, ClientConstraintType.UNIQUE_KEY);
    }
}