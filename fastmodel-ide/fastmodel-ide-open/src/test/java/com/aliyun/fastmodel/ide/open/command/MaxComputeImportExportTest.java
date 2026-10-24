/*
 * Copyright 2021-2022 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.aliyun.fastmodel.ide.open.command;

import java.util.Collections;

import com.aliyun.fastmodel.ide.open.extension.OpenIdeCommandProvider;
import com.aliyun.fastmodel.ide.open.service.StorageService;
import com.aliyun.fastmodel.ide.open.start.controller.CommandController;
import com.aliyun.fastmodel.ide.spi.command.IdeCommand;
import com.aliyun.fastmodel.ide.spi.invoker.IdeInvoker;
import com.aliyun.fastmodel.ide.spi.receiver.IdePlatform;
import com.aliyun.fastmodel.ide.spi.receiver.impl.DefaultToolServiceImpl;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Exercise HTTP form binding, command discovery and the actual MaxCompute transformer together.
 */
public class MaxComputeImportExportTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    private MockMvc mvc;

    @Before
    public void setUp() {
        IdePlatform platform = mock(IdePlatform.class);
        when(platform.getToolService()).thenReturn(new DefaultToolServiceImpl());
        OpenIdeCommandProvider provider = new OpenIdeCommandProvider(platform);
        ApplicationContext applicationContext = mock(ApplicationContext.class);
        when(applicationContext.getBeansOfType(IdeCommand.class)).thenReturn(Collections.emptyMap());
        provider.setApplicationContext(applicationContext);
        provider.init();
        CommandController controller = new CommandController(provider, mock(IdeInvoker.class),
            mock(StorageService.class));
        mvc = MockMvcBuilders.standaloneSetup(controller).build();
    }

    @Test
    public void importsAndExportsMaxComputeThroughController() throws Exception {
        String sql = "CREATE TABLE sample_product (product_id BIGINT, product_name STRING) "
            + "COMMENT 'Example products' PARTITIONED BY (ds STRING) LIFECYCLE 7;";
        String fml = importSql("MaxCompute", sql);
        String normalizedFml = normalizeWhitespace(fml);
        assertTrue(fml, normalizedFml.contains("CREATE DIM TABLE sample_product"));
        assertTrue(fml, normalizedFml.contains("product_id BIGINT"));
        assertTrue(fml, normalizedFml.contains("product_name STRING"));

        String exported = exportSql("MaxCompute", fml);
        String normalizedExport = normalizeWhitespace(exported);
        assertTrue(exported, normalizedExport.contains("CREATE TABLE sample_product"));
        assertTrue(exported, normalizedExport.contains("product_id BIGINT"));
        assertTrue(exported, normalizedExport.contains("product_name STRING"));
        assertTrue(exported, exported.contains("COMMENT 'Example products'"));
        assertTrue(exported, normalizedExport.contains("PARTITIONED BY"));
        assertTrue(exported, normalizedExport.contains("ds STRING"));
        assertTrue(exported, normalizedExport.contains("LIFECYCLE 7"));
        assertEquals(fml, importSql("MaxCompute", exported));
    }

    @Test
    public void resolvesOdpsAliasThroughController() throws Exception {
        String sql = "CREATE TABLE sample_item (item_id BIGINT);";
        String fml = importSql("ODPS", sql);
        assertEquals(importSql("MaxCompute", sql), fml);
        assertEquals(exportSql("MaxCompute", fml), exportSql("ODPS", fml));
    }

    private String normalizeWhitespace(String sql) {
        return sql.replaceAll("\\s+", " ").trim();
    }

    private String importSql(String dialect, String text) throws Exception {
        MvcResult result = mvc.perform(post("/engine/import_sql")
                .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                .param("dialectName", dialect)
                .param("text", text))
            .andExpect(status().isOk()).andReturn();
        return successfulData(result);
    }

    private String exportSql(String dialect, String fml) throws Exception {
        MvcResult result = mvc.perform(post("/engine/export_sql")
                .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                .param("dialectName", dialect)
                .param("fml", fml))
            .andExpect(status().isOk()).andReturn();
        return successfulData(result);
    }

    private String successfulData(MvcResult result) throws Exception {
        JsonNode response = objectMapper.readTree(result.getResponse().getContentAsByteArray());
        assertTrue(response.toString(), response.path("success").asBoolean());
        assertTrue(response.toString(), response.path("data").isTextual());
        return response.path("data").asText();
    }
}
