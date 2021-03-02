package solcolator.io.readers;


import org.apache.solr.common.util.NamedList;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static solcolator.io.readers.FileReader.FILE_PATH;


class FileReaderTest {

  private static final String NO_FILE = "nonexistingFile";
  private static final String QUERY_FILE = "testQueryFile.json";
  private static final String MALFORMED_FILE = "malformedQueryFile.json";
  private static final int TEST_QUERY_COUNT = 4;

  @Test()
  void shouldThrowExceptionIfFilePathNotSet() {
    var fileReader = new FileReader();
    var exception = assertThrows(IllegalArgumentException.class, () ->
      fileReader.init(new NamedList<>()));
    assertEquals(String.format("Must set %s in config", FILE_PATH), exception.getMessage());
  }

  @Test()
  void shouldThrowExceptionIfFilePathDoesNotExist() {
    var fileReader = new FileReader();
    var inputConfig = new NamedList<String>();
    inputConfig.add(FILE_PATH, NO_FILE);
    var exception = assertThrows(IllegalArgumentException.class, () ->
      fileReader.init(inputConfig));
    assertEquals(String.format("File %s doesn't exist", NO_FILE), exception.getMessage());
  }

  @Test
  void shouldReturnEmptyListIfNotInitialized() {
    var fileReader = new FileReader();
    var queries = fileReader.readAllQueries(Collections.singletonMap("key", "value"));
    assertEquals(0, queries.size());
  }

  @Test
  void shouldReadAllQueries() {
    var fileReader = createTestFileReader(QUERY_FILE);
    var queries = fileReader.readAllQueries(Collections.singletonMap("key", "value"));
    assertEquals(TEST_QUERY_COUNT, queries.size());
  }

  @Test
  void shouldThrowExceptionWhenFileMalformed() {
    var fileReader = createTestFileReader(MALFORMED_FILE);
    var exception = assertThrows(ExceptionInInitializerError.class, () ->
      fileReader.readAllQueries(Collections.singletonMap("key", "value")));
    assertTrue(exception.getMessage().startsWith("Failed to read queries"));
    assertTrue(exception.getMessage().contains("JsonSyntaxException"));
  }

  @Test
  void shouldReadByQueryId() throws IOException {
    var fileReader = createTestFileReader(QUERY_FILE);
    var query = fileReader.readByQueryId("2", "name", Collections.singletonMap("key", "value"));
    assertEquals("2", query.getQueryId());
    assertEquals("test2", query.getQueryName());
  }

  @Test
  void shouldThrowExceptionIfQueryIdNotFound() {
    var fileReader = createTestFileReader(QUERY_FILE);
    var invalidId = "4";
    var exception = assertThrows(IOException.class, () ->
      fileReader.readByQueryId(invalidId, "name", Collections.singletonMap("key", "value")));
    assertEquals(String.format("Query with id %s wasn't found", invalidId), exception.getMessage());
  }

  @Test
  void shouldThrowExceptionIfDuplicateQueryIdFound() {
    var fileReader = createTestFileReader(QUERY_FILE);
    var duplicateId = "3";
    var exception = assertThrows(IOException.class, () ->
      fileReader.readByQueryId(duplicateId, "name", Collections.singletonMap("key", "value")));
    assertEquals(String.format("Found %d queries with id %s", 2, duplicateId), exception.getMessage());
  }

  @Test
  void shouldNotThrowOnClose() {
    var fileReader = createTestFileReader(QUERY_FILE);
    assertDoesNotThrow(fileReader::close);
  }

  private FileReader createTestFileReader(final String queryFile) {
    var classLoader = getClass().getClassLoader();
    var file = new File(Objects.requireNonNull(classLoader.getResource(queryFile)).getFile());
    var absolutePath = file.getAbsolutePath();
    var fileReader = new FileReader();
    var inputConfig = new NamedList<String>();
    inputConfig.add(FILE_PATH, absolutePath);
    fileReader.init(inputConfig);
    assertEquals(file, fileReader.getFile());
    return fileReader;
  }

}
