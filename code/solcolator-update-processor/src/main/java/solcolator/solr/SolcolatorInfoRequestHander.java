package solcolator.solr;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.SearchHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import solcolator.monitor.LuwakQueriesManager;
import solcolator.monitor.LuwakQuery;

import java.util.Map;
import java.util.Map.Entry;

/**
 * The handler for developer & QA tests. It displays the current state of solcolator, which queries are saved in.
 *
 */
public class SolcolatorInfoRequestHander extends SearchHandler {
	private final static String NUMBER_QUERIES_IN_SOLCOLATOR_HEADER = "numberQueriesInSolcolator";
	
	@Override
	public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {						
		LuwakQueriesManager manager = LuwakQueriesManager.getQueriesManager();
		Map<String, LuwakQuery> queriesMap = manager.getQueryIdToLuwakQuery();
		
		NamedList<Object> nmsLst = new NamedList<Object>();
		nmsLst.add(NUMBER_QUERIES_IN_SOLCOLATOR_HEADER, manager.getMonitor().getQueryCount());
		nmsLst.add(SolcolatorQueriesRequestHander.NAME, SolcolatorQueriesRequestCommand.toPrint());
		
		rsp.addResponseHeader(nmsLst);

		getAllQueriesInPrintableFormat(queriesMap, rsp);
	}
	
	private void getAllQueriesInPrintableFormat(Map<String, LuwakQuery> queriesMap, SolrQueryResponse rsp) {
		for (Entry<String, LuwakQuery> query : queriesMap.entrySet()) {
			rsp.add(query.getKey(),query.getValue().getQuery());
		}
	}
}
