package enridaga.basil.server.experimental;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;

import uk.ac.open.kmi.basil.core.InvocationResult;
import uk.ac.open.kmi.basil.core.exceptions.ApiInvocationException;
import uk.ac.open.kmi.basil.invoke.QueryExecutor;

public class CachedExecutor implements QueryExecutor {
	
	private static final Logger log = LoggerFactory.getLogger(CachedExecutor.class);
	static{
		log.info("Loading CachedExecutor");
	}
	
	@Override
	public InvocationResult execute(Query q, String endpoint) throws ApiInvocationException {
		log.info("Invoking cached executor");
		QueryExecution qe = QueryExecutionFactory.sparqlService(endpoint, q);
		if (q.isSelectType()) {
			return new InvocationResult(qe.execSelect(), q);
		} else if (q.isConstructType()) {
			return new InvocationResult(qe.execConstruct(), q);
		} else if (q.isAskType()) {
			return new InvocationResult(qe.execAsk(), q);
		} else if (q.isDescribeType()) {
			return new InvocationResult(qe.execDescribe(), q);
		} else {
			throw new ApiInvocationException("Unsupported query type: " + q.getQueryType());
		}
	}

}
