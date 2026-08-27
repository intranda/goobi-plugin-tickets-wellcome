package org.goobi.api.mq;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class ImportEPHandlerTest {

    private static final String HEADER = "Title,Shoot Type,Reference,Location,Staff Photog,Freelancer,Caption,People,Keywords,Intended Usage,Usage Terms";
    private static final String LINE =
            "Snakehack Nairobi,Public Engagement,AH 000511,Nairobi,,Billy Miaron,\"Wellcome Snakebite Innovation Prize SnakeHack event in Nairobi, August 2026\",,Snakebite Nairobi Panel,,";

    @TempDir
    Path tempDir;

    @Test
    public void testReadFileWithUtf8Bom() throws IOException {
        // Excel and other tools prepend a UTF-8 BOM. It must not become part of the first column name.
        Path csvFile = writeCsv("bom.csv", "﻿" + HEADER + "\n" + LINE + "\n");

        Map<String, Integer> indexMap = new HashMap<>();
        List<String[]> values = new ArrayList<>();
        new ImportEPHandler().readFile(csvFile, indexMap, values);

        assertEquals(Integer.valueOf(0), indexMap.get("Title"));
        assertEquals("Snakehack Nairobi", new ImportEPHandler().getValue("Title", indexMap, values));
        assertEquals("AH 000511", new ImportEPHandler().getValue("Reference", indexMap, values));
    }

    @Test
    public void testReadFileWithoutBom() throws IOException {
        Path csvFile = writeCsv("plain.csv", HEADER + "\n" + LINE + "\n");

        Map<String, Integer> indexMap = new HashMap<>();
        List<String[]> values = new ArrayList<>();
        new ImportEPHandler().readFile(csvFile, indexMap, values);

        assertEquals(Integer.valueOf(0), indexMap.get("Title"));
        assertEquals("Snakehack Nairobi", new ImportEPHandler().getValue("Title", indexMap, values));
        assertEquals("Wellcome Snakebite Innovation Prize SnakeHack event in Nairobi, August 2026",
                new ImportEPHandler().getValue("Caption", indexMap, values));
    }

    private Path writeCsv(String name, String content) throws IOException {
        Path csvFile = tempDir.resolve(name);
        Files.write(csvFile, content.getBytes(StandardCharsets.UTF_8));
        return csvFile;
    }
}
