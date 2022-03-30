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

package com.aliyun.fastmodel.parser.pipe;

import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.pipe.CopyMode;
import com.aliyun.fastmodel.core.tree.statement.pipe.CreatePipe;
import com.aliyun.fastmodel.core.tree.statement.pipe.PipeCopyInto;
import com.aliyun.fastmodel.core.tree.statement.pipe.PipeFrom;
import com.aliyun.fastmodel.core.tree.statement.pipe.PipeType;
import com.aliyun.fastmodel.core.tree.statement.pipe.TargetType;
import com.aliyun.fastmodel.parser.BaseTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/4/6
 */
public class CreatePipeTest extends BaseTest {

    @Test
    public void testCreatePipeSyncInclude() {
        String fml
            =
            "CREATE SYNC PIPE IF NOT EXISTS a.b COMMENT 'comment' AS COPY INTO table WITH(COPY_MODE='REPLACE') FROM "
                + "maxcompute.meta"
                + " WHERE Source='hmcdm' and "
                + "(code LIKE 'dim*' OR code like 'dim1')"
                + " and BIZ_DATE='20200101'";
        CreatePipe createPipe = nodeParser.parseStatement(fml);
        assertNotNull(createPipe);
        assertEquals(createPipe.getPipeType(), PipeType.SYNC);
        assertNotNull(createPipe.getPipeFrom());
        PipeCopyInto pipeCopyInto = createPipe.getPipeCopyInto();
        assertEquals(pipeCopyInto.getTargetType(), TargetType.TABLE);
        CopyMode copyMode = pipeCopyInto.getCopyMode();
        assertEquals(copyMode, CopyMode.REPLACE);

        PipeFrom pipeFrom = createPipe.getPipeFrom();
        QualifiedName from = pipeFrom.getFrom();
        assertEquals(from.getSuffix(), "meta");

    }

    @Test
    public void testCreatePipeSyncNoWIth() {
        String fml
            =
            "CREATE SYNC PIPE IF NOT EXISTS a.b COMMENT 'comment' AS COPY INTO table FROM "
                + "maxcompute.meta"
                + " WHERE Source='hmcdm' and "
                + "(code LIKE 'dim*' OR code like 'dim1')"
                + " and BIZ_DATE='20200101'";
        CreatePipe createPipe = nodeParser.parseStatement(fml);
        assertNotNull(createPipe);
        assertEquals(createPipe.getPipeType(), PipeType.SYNC);
        assertNotNull(createPipe.getPipeFrom());
        PipeCopyInto pipeCopyInto = createPipe.getPipeCopyInto();
        assertEquals(pipeCopyInto.getTargetType(), TargetType.TABLE);
        CopyMode copyMode = pipeCopyInto.getCopyMode();
        assertEquals(copyMode, CopyMode.REPLACE);

        PipeFrom pipeFrom = createPipe.getPipeFrom();
        QualifiedName from = pipeFrom.getFrom();
        assertEquals(from.getSuffix(), "meta");

    }
}
