package solcolator.io.writers;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static solcolator.io.writers.FileWriter.FILE_FL;
import static solcolator.io.writers.FileWriter.FILE_PATH;

class FileWriterTest {

  @TempDir File tmpDir;
  private static final String FILENAME = "testOutput.json";
  private static final String FIELD_LIST = "id,name,category";

  @AfterEach
  void teardown() {
    var tmpFile = Path.of(tmpDir.getAbsolutePath(), FILENAME);
    //noinspection ResultOfMethodCallIgnored
    tmpFile.toFile().delete();
  }

  @Test
  void shouldOpenFileForWriting() throws IOException {
    var path = Path.of(tmpDir.getAbsolutePath(), FILENAME);
    assertFalse(Files.exists(path));
    createTestFileWriter();
    assertTrue(Files.exists(path));
  }

  @Test
  void shouldMaintainFieldList() throws IOException {
    var fileWriter = createTestFileWriter();
    assertEquals(Arrays.asList(FIELD_LIST.split(",")), fileWriter.getFl());
  }

  @Test
  void shouldThrowExceptionWhenInvalidFileSpecified() {
    var fileWriter = new FileWriter();
    var outputConfig = new NamedList<String>();
    var path = Path.of(tmpDir.getAbsolutePath(), FILENAME);
    outputConfig.add(FILE_PATH, path.toString());
    outputConfig.add(FILE_FL, FIELD_LIST);
    teardown();
    assertTrue(tmpDir.delete());
    var exception = assertThrows(IOException.class, () -> fileWriter.init(outputConfig));
    assertTrue(exception.getLocalizedMessage().endsWith("(No such file or directory)"));
  }

  @Test
  void shouldThrowExceptionIfNoFieldListConfigured() {
    var fileWriter = new FileWriter();
    var outputConfig = new NamedList<String>();
    var path = Path.of(tmpDir.getAbsolutePath(), FILENAME);
    outputConfig.add(FILE_PATH, path.toString());
    var exception = assertThrows(IllegalArgumentException.class, () -> fileWriter.init(outputConfig));
    assertTrue(exception.getLocalizedMessage().startsWith(String.format("%s and %s must be configured", FILE_PATH, FILE_FL)));
  }

  @Test
  void writeSolcolatorResults() throws IOException {
    Map<String, List<SolrInputDocument>> queriesToDocs = new HashMap<>();
    var fields = FIELD_LIST.split(",");
    int numTestQueries = 3;
    int numTestMatches = 3;

    for (int i = 1; i <= numTestQueries; i++) {
      List<SolrInputDocument> list = new ArrayList<>();
      for (int j = 1; j <= numTestMatches; j++) {
        var doc = new SolrInputDocument();
        for (String field : fields) {
          doc.addField(field, field + i*j);
        }
        list.add(doc);
      }
      queriesToDocs.put("query" + i, list);
    }

    var fileWriter = createTestFileWriter();
    fileWriter.writeSolcolatorResults(queriesToDocs);
    fileWriter.close();

    String fileContent = Files.readString(Path.of(tmpDir.getAbsolutePath(), FILENAME), StandardCharsets.UTF_8);
    Type type = new TypeToken<Map<String, List<SolrInputDocument>>>() { }.getType();
    Gson gson = new Gson();
    Map<String, List<SolrInputDocument>> matches = gson.fromJson(fileContent, type);
    assertEquals(numTestQueries, matches.size());
    matches.forEach((k,v) -> assertEquals(numTestMatches, v.size()));
  }

  private FileWriter createTestFileWriter() throws IOException {
    var fileWriter = new FileWriter();
    var outputConfig = new NamedList<String>();
    var path = Path.of(tmpDir.getAbsolutePath(), FILENAME);
    outputConfig.add(FILE_PATH, path.toString());
    outputConfig.add(FILE_FL, FIELD_LIST);
    fileWriter.init(outputConfig);
    return fileWriter;
  }

}
