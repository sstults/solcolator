package solcolator.io.readers;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.solr.common.util.NamedList;
import solcolator.io.api.IQueryReader;
import solcolator.io.api.SolcolatorQuery;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Query reader from json file
 * 
 * File Reader Config:
 * <lst name="reader">
		<str name="class">solcolator.io.readers.FileReader</str>
		<str name="filePath">[full path to file with queries]</str>
	</lst>
 * 
 * Query file for example:
 * [
		{
			"query_id": "1",
			"query_name": "test",
			"query": "q=price:[100 TO 200]"
		}
   ]
 */
public class FileReader implements IQueryReader {
	public static final String FILE_PATH = "filePath";
	private final Type type = new TypeToken<FileQueryObject[]>() { }.getType();
	private final Gson gson = new Gson();
	private File file;
	
	public void init(NamedList<?> inputConfig) {
		String filePath = (String) inputConfig.get(FILE_PATH);

		if(Objects.isNull(filePath)) {
			throw new IllegalArgumentException(String.format("Must set %s in config", FILE_PATH));
		}

		file = new File(filePath);
		
		if (!file.exists()) {
			throw new IllegalArgumentException(String.format("File %s doesn't exist", filePath));
		}
	}

	public List<SolcolatorQuery> readAllQueries(Map<String, String> reqHandlerMetadata) {
		if (file == null) {
			return new ArrayList<>();
		}
		
		FileQueryObject[] queriesObjects;
		List<SolcolatorQuery> solcolatorQueries;
		String filePath = file.getAbsolutePath();
		
		try {
			String fileContent = Files.readString(Paths.get(filePath), StandardCharsets.UTF_8);
			queriesObjects = gson.fromJson(fileContent, type);
			
			solcolatorQueries = new ArrayList<>(queriesObjects.length);
			for (FileQueryObject obj : queriesObjects) {
				solcolatorQueries.add(new SolcolatorQuery(obj.query_id, obj.query_name, obj.query, reqHandlerMetadata));
			}			
		} catch (Exception e) {
			throw new ExceptionInInitializerError(String.format("Failed to read queries from file %s due to %s", filePath, e));
		}

		return solcolatorQueries;
	}

	//TODO: remove queryName from signature
	//TODO: return Optional.of(SolcolatorQuery) instead of throwing exceptions
	public SolcolatorQuery readByQueryId(final String queryId, String queryName, Map<String, String> reqHandlerMetadata) throws IOException {
		List<SolcolatorQuery> queries = readAllQueries(reqHandlerMetadata);
		
		Supplier<List<SolcolatorQuery>> supplier = ArrayList::new;
		List<SolcolatorQuery> foundQueries = queries.stream().filter((SolcolatorQuery x) -> x.getQueryId().equals(queryId)).collect(Collectors.toCollection(supplier));
		
		if (foundQueries.isEmpty()) {
			throw new IOException(String.format("Query with id %s wasn't found", queryId));
		}
		
		if (foundQueries.size() > 1) {
			throw new IOException(String.format("Found %d queries with id %s", foundQueries.size(), queryId));
		}

		return foundQueries.get(0);
	}
	
	private static class FileQueryObject {
		public String query_id;
		public String query_name;
		public String query;
	}

	public File getFile() {
		return file;
	}

	public void close() {
		//Nothing to close
	}

}
