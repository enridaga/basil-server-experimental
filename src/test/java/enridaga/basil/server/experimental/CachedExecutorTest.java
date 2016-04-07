package enridaga.basil.server.experimental;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.open.kmi.basil.core.InvocationResult;
import uk.ac.open.kmi.basil.core.exceptions.ApiInvocationException;

public class CachedExecutorTest {
	private static final Logger l = LoggerFactory.getLogger(CachedExecutorTest.class);

	@Test
	public void test() throws ApiInvocationException {
		CachedExecutor executor = new CachedExecutor();
		String q = "# X-Basil-Endpoint: http://data.open.ac.uk/sparql\n" + 
				"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" + 
				"PREFIX mlo: <http://purl.org/net/mlo/> \n" + 
				"PREFIX aiiso: <http://purl.org/vocab/aiiso/schema#> \n" + 
				"\n" + 
				"# Eg value for ?_geoid 2328926\n" + 
				"SELECT ?course FROM <http://data.open.ac.uk/context/course> \n" + 
				"WHERE {\n" + 
				" ?course mlo:location ?location . ?course a aiiso:Module \n" + 
				"}";
		Query qe =  QueryFactory.create(q);
		try {
			InvocationResult r = executor.execute(qe, "http://data.open.ac.uk/sparql");
			Assert.assertTrue(r.getResult() instanceof ResultSet);
			ResultSet rs = (ResultSet) r.getResult();
			Assert.assertTrue(rs.hasNext());
			if(l.isDebugEnabled()){
				while(rs.hasNext()){
					l.info("{}",rs.next());
				}
			}
		} catch (ApiInvocationException e) {
			throw e;
		}
	}
	
	@Test
	public void testCache() throws ApiInvocationException {
		CachedExecutor executor = new CachedExecutor();
		String q = "# X-Basil-Endpoint: http://data.open.ac.uk/sparql\n" + 
				"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" + 
				"PREFIX mlo: <http://purl.org/net/mlo/> \n" + 
				"PREFIX aiiso: <http://purl.org/vocab/aiiso/schema#> \n" + 
				"\n" + 
				"# Eg value for ?_geoid 2328926\n" + 
				"SELECT ?course FROM <http://data.open.ac.uk/context/course> \n" + 
				"WHERE {\n" + 
				" ?course mlo:location ?location . ?course a aiiso:Module \n" + 
				"}";
		Query qe =  QueryFactory.create(q);
		try {
			long start = System.currentTimeMillis();
			executor.execute(qe, "http://data.open.ac.uk/sparql");
			long time1 = System.currentTimeMillis() - start;
			l.info("#1 Executed in {}ms", time1);
			start = System.currentTimeMillis();
			executor.execute(qe, "http://data.open.ac.uk/sparql");
			long time2 = System.currentTimeMillis() - start;
			l.info("#2 Executed in {}ms", time1);
			Assert.assertTrue(time2 < time1);
		} catch (ApiInvocationException e) {
			throw e;
		}
	}
}
