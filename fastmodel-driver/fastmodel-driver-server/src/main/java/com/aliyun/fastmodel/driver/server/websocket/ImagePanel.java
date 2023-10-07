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

package com.aliyun.fastmodel.driver.server.websocket;

import java.awt.*;
import java.awt.image.BufferedImage;

import javax.swing.*;

import lombok.Setter;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/2/9
 */
@Setter
public class ImagePanel extends JPanel {
    public static final int TILED = 0;
    public static final int SCALED = 1;
    public static final int ACTUAL = 2;

    private BufferedImage image;
    private int style;
    private float alignmentX = 0.5f;
    private float alignmentY = 0.5f;

    public ImagePanel(BufferedImage image) {
        this(image, ACTUAL);
    }

    public ImagePanel(BufferedImage image, int style) {
        this.image = image;
        this.style = style;
        setLayout(new BorderLayout());
    }

    public void setImageAlignmentX(float alignmentX) {
        this.alignmentX = alignmentX > 1.0f ? 1.0f : alignmentX < 0.0f ? 0.0f
            : alignmentX;
    }

    public void setImageAlignmentY(float alignmentY) {
        this.alignmentY = alignmentY > 1.0f ? 1.0f : alignmentY < 0.0f ? 0.0f
            : alignmentY;

    }

    public void add(JComponent component) {
        add(component, null);
    }

    public void add(JComponent component, Object constraints) {
        component.setOpaque(false);

        if (component instanceof JScrollPane) {
            JScrollPane scrollPane = (JScrollPane)component;
            JViewport viewport = scrollPane.getViewport();
            viewport.setOpaque(false);
            Component c = viewport.getView();

            if (c instanceof JComponent) {
                ((JComponent)c).setOpaque(false);
            }
        }

        super.add(component, constraints);
    }

    @Override
    protected void paintComponent(Graphics g) {
        super.paintComponent(g);

        if (image == null) {return;}

        switch (style) {
            case TILED:
                drawTiled(g);
                break;

            case SCALED:
                Dimension d = getSize();
                g.drawImage(image, 0, 0, d.width, d.height, null);
                break;

            case ACTUAL:
                drawActual(g);
                break;
            default:
                break;
        }
    }

    private void drawTiled(Graphics g) {
        Dimension d = getSize();
        int width = image.getWidth(null);
        int height = image.getHeight(null);

        for (int x = 0; x < d.width; x += width) {
            for (int y = 0; y < d.height; y += height) {
                g.drawImage(image, x, y, null, null);
            }
        }
    }

    private void drawActual(Graphics g) {
        Dimension d = getSize();
        float x = (d.width - image.getWidth()) * alignmentX;
        float y = (d.height - image.getHeight()) * alignmentY;
        g.drawImage(image, (int)x, (int)y, this);
    }
}
