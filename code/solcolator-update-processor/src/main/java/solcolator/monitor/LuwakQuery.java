package solcolator.monitor;

import org.apache.lucene.monitor.MonitorQuery;

import java.util.Map;

/**
 * The class represents json serializable LUWAK query
 */
public class LuwakQuery extends MonitorQuery {
	private final String queryName;
	
	public LuwakQuery(String id, String queryName, String query, Map<String, String> queryMetadata) {
		super(id, null, query, queryMetadata);
		this.queryName = queryName;
	}
	
	public String getQueryName() {
		return queryName;
	}
}
