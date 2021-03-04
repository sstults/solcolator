package solcolator.io.writers;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import solcolator.io.api.ISolcolatorResultsWriter;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * This writer is designed to write solcolator results to a file
 * This writer can be used for testing and integration purposes
 * 
 * File Writer Config:
 	<lst>
		<str name="class">solcolator.io.writers.FileWriter</str>
		<str name="filePath">[full file path with results (.txt or .csv)</str>
		<str name="fileFl">[comma separated list of fields are separated]</str>
	</lst>
 */
public class FileWriter implements ISolcolatorResultsWriter {
	public static final String FILE_PATH = "filePath";
	public static final String FILE_FL = "fileFl";

	private final Gson gson = new GsonBuilder().disableHtmlEscaping().create();
	private static final Logger log = LoggerFactory.getLogger(FileWriter.class);

	private List<String> fl;
	private BufferedWriter bw;
	
	public void init(NamedList<?> outputConfig) throws IOException {
		String flString = (String) outputConfig.get(FILE_FL);
		String filePath = (String) outputConfig.get(FILE_PATH);

		if (Objects.isNull(flString) || Objects.isNull(filePath)) {
			throw new IllegalArgumentException(String.format("%s and %s must be configured, but were [%s] and [%s]",
				FILE_PATH, FILE_FL, filePath, flString));
		}

		fl = Arrays.asList(flString.split(","));
		bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filePath, false), StandardCharsets.UTF_8));
	}
	
	public void writeSolcolatorResults(Map<String, List<SolrInputDocument>> queriesToDocs) {
		try {
			bw.append(String.format("%s\n", gson.toJson(queriesToDocs)));
			bw.flush();
		} catch (IOException ex) {
			log.error("Writing results to file is failed", ex);
		}
	}
	
	public List<String> getFl() {
		return fl;
	}

	public void close() throws IOException {
		if (bw != null) {
			bw.close();
		}
	}
}
