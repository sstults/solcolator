package solcolator.io.writers;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import solcolator.io.api.ISolcolatorResultsWriter;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * NOTE! IT'S A BASIC IMPLEMENTATION ONLY. DON'T USE THIS WRITER IN PRODUCTION ENVIRONMENT WITHOUT ADDITIONAL TESTS!
 * 
 * This writer is designed to write solcolator results to another SOLR collection
 * Parameters which must be provided: zookeepers, collection name and fields list
 * 
 * Collection Writer Config:
    <lst>
		<str name="class">solcolator.io.writers.CollectionWriter</str>
		<str name="solrUrls">[comma separated list of Solr URLs, eg: "http://solr1:8983/solr"]</str>
		<str name="collectionName">[collection name]</str>
		<str name="collectionFl">[comma separated list of fields are separated]</str>
	</lst>
 *
 */
public class CollectionWriter implements ISolcolatorResultsWriter {
	private static final Logger log = LoggerFactory.getLogger(CollectionWriter.class);
	private static final String COLLECTION_NAME = "collectionName";
	private static final String COLLECTION_FL = "collectionFl";
	public static final String SOLR_URLS = "solrUrls";
	
	private List<String> fl;
	private CloudSolrClient solrClient;

	public void init(NamedList<?> outputConfig) {
		String solrUrls = (String) outputConfig.get(SOLR_URLS);
		String collection = (String) outputConfig.get(COLLECTION_NAME);
		this.fl = Arrays.asList(((String) outputConfig.get(COLLECTION_FL)).split(","));
		this.solrClient = new CloudSolrClient.Builder(Arrays.asList(solrUrls.split(","))).build();
		this.solrClient.setDefaultCollection(collection);
	}

	public void writeSolcolatorResults(Map<String, List<SolrInputDocument>> queriesToDocs) throws IOException {
		for (Entry<String, List<SolrInputDocument>> queryToDocs : queriesToDocs.entrySet()) {
			try {
				solrClient.add(queryToDocs.getValue());
			} catch (SolrServerException e) {
				log.error(String.format("Bulk of %d docs of query %s failed to index to collection %s@%s",
						queryToDocs.getValue().size(),
						queryToDocs.getKey(),
						solrClient.getDefaultCollection(),
						solrClient.getZkHost()
						), e);
			}
		}			
	}

	public List<String> getFl() {
		return fl;
	}
	
	public void close() throws IOException {
		solrClient.close();
	}
}
