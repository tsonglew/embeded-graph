package com.tsonglew.embeddedGraph;

import groovy.util.logging.Slf4j;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraphFactory;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.logging.Level;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.File;
import java.nio.file.Path;
import java.time.Duration;


@SpringBootApplication
public class EmbeddedGraphApplication implements ApplicationRunner {

	public static void main(String[] args) {
		SpringApplication.run(EmbeddedGraphApplication.class, args);
	}

	@Override
	public void run(ApplicationArguments args) throws Exception {
//		testJanusGraph();
		testNeo4j();
	}

	public void testNeo4j() throws Exception {
		var testStartTime = System.currentTimeMillis();
		DatabaseManagementService managementService = new DatabaseManagementServiceBuilder(Path.of("src/main/db/neo4j"))
				.setConfig(GraphDatabaseSettings.pagecache_memory, "2G")
				.setConfig(GraphDatabaseSettings.transaction_timeout, Duration.ofSeconds(60))
				.setConfig(GraphDatabaseSettings.store_internal_log_level, Level.DEBUG)
				.build();
		GraphDatabaseService graphDb = managementService.database("neo4j");
		System.out.println("init test time: " + (System.currentTimeMillis()-testStartTime));
		try (var tx = graphDb.beginTx()) {
			var createNodeStart = System.currentTimeMillis();
			Node lastNode = null;
			for (var i = 0; i < 10000; i++) {
				var n = tx.createNode(Label.label("person"));
				n.setProperty("name", "bob"+i);
				n.setProperty("age", i);
				if (lastNode != null) {
					n.createRelationshipTo(lastNode, RelTypes.KNOWS);
				}
				lastNode = n;
			}
			tx.commit();

			System.out.println("create node time: " + (System.currentTimeMillis()-createNodeStart));
		}

		try (var tx = graphDb.beginTx()) {
			System.out.println("nodes count: " + tx.getAllNodes().stream().count());
			System.out.println("edges count: " + tx.getAllRelationships().stream().count());
			var queryStarTime = System.currentTimeMillis();
			System.out.println(tx.findNode(Label.label("person"), "name", "bob10")
					.getSingleRelationship(RelTypes.KNOWS, Direction.OUTGOING)
					.getEndNode()
					.getProperties("name"));
			System.out.println("1 jump time: " + (System.currentTimeMillis()- queryStarTime));

			var queryStarTime2 = System.currentTimeMillis();
			System.out.println(tx.findNode(Label.label("person"), "name", "bob10")
					.getSingleRelationship(RelTypes.KNOWS, Direction.OUTGOING)
					.getEndNode()
							.getSingleRelationship(RelTypes.KNOWS, Direction.OUTGOING)
							.getEndNode()
					.getProperties("name"));
			System.out.println("2 jump time: " + (System.currentTimeMillis()- queryStarTime2));
		}
	}

	public void testJanusGraph() throws Exception{
		try (Graph graph = JanusGraphFactory.open("src/main/resources/embedded.properties")) {
			GraphTraversalSource g = graph.traversal();
//			g.V().drop().iterate();
			var start = System.currentTimeMillis();

			Vertex lastV = null;
			for (var n = 0; n < 100000; n++) {
				var newV = g.addV("person").property("name", "bob"+n).property("age", n).next();
				if (lastV != null) {
					g.addE("father").from(lastV).to(newV).iterate();
				}
				lastV = newV;
			}
			g.tx().commit();
			System.out.println("import data time: " + (System.currentTimeMillis() - start));


			var queryStart = System.currentTimeMillis();
			System.out.println("bob5: " + g.V().has("name", "bob5").out("father").out("father").next().value("name"));
			System.out.println("query data time: " + (System.currentTimeMillis() - queryStart));

//			List<Map<Object, Object>> result = g.V().valueMap("name", "age").toList();

//			for(Map<Object, Object> vertex : result){
//				ArrayList<String> names = (ArrayList<String>)vertex.get("name");
//				ArrayList<Integer> ages = (ArrayList<Integer>)vertex.get("age");
//				String name = names.get(0);
//				Integer age = ages.get(0);
//				System.out.printf("name: %s, age: %s\n", name, age);
//			}
			//Try issuing a query to retrieve the number of vertices
			Long count = g.V().count().next();
			System.out.printf("vertex count is %d\n", count);
			System.out.printf("edge count is %d\n", g.E().count().next());

		}
	}
}
