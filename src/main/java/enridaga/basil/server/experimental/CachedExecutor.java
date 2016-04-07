package enridaga.basil.server.experimental;

import org.aksw.jena_sparql_api.concept_cache.core.OpExecutionFactoryViewCache;
import org.aksw.jena_sparql_api.concept_cache.core.QueryExecutionFactoryViewCacheMaster;
import org.aksw.jena_sparql_api.core.FluentQueryExecutionFactory;
import org.aksw.jena_sparql_api.core.QueryExecutionFactory;
import org.aksw.jena_sparql_api.stmt.SparqlQueryParserImpl;
import org.aksw.jena_sparql_api.utils.transform.F_QueryTransformDatesetDescription;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
//import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.Syntax;
import org.apache.jena.sparql.engine.main.QC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.open.kmi.basil.core.InvocationResult;
import uk.ac.open.kmi.basil.core.exceptions.ApiInvocationException;
import uk.ac.open.kmi.basil.invoke.QueryExecutor;

public class CachedExecutor implements QueryExecutor {
	private static OpExecutionFactoryViewCache opExecutionFactory;

	private static final Logger log = LoggerFactory.getLogger(CachedExecutor.class);

	static {
		log.info("Loading CachedExecutor");
		ARQ.init();
		opExecutionFactory = new OpExecutionFactoryViewCache();
		QC.setFactory(ARQ.getContext(), opExecutionFactory);
	}

	public CachedExecutor() {

	}

	@Override
	public InvocationResult execute(Query q, String endpoint) throws ApiInvocationException {
		log.info("Invoking cached executor");
		QueryExecutionFactory baseQef = //QueryExecutionFactory.sparqlService(endpoint, q);
				FluentQueryExecutionFactory.http(endpoint)
				.config()
		        .withParser(SparqlQueryParserImpl.create(Syntax.syntaxARQ))
		        .withQueryTransform(F_QueryTransformDatesetDescription.fn).end().create();
		QueryExecutionFactory qef = new QueryExecutionFactoryViewCacheMaster(baseQef, opExecutionFactory.getServiceMap());
		QueryExecution qe = qef.createQueryExecution(q);
		if (q.isSelectType()) {
			return new InvocationResult(qe.execSelect(), q);
//		} else
//			if (q.isConstructType()) {
//			return new InvocationResult(qe.execConstruct(), q);
//		} else if (q.isAskType()) {
//			return new InvocationResult(qe.execAsk(), q);
//		} else if (q.isDescribeType()) {
//			return new InvocationResult(qe.execDescribe(), q);
		} else {
			throw new ApiInvocationException("Unsupported query type: " + q.getQueryType());
		}
	}

}
