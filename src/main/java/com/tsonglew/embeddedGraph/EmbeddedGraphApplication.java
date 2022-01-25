package com.tsonglew.embeddedGraph;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraphFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


@SpringBootApplication
public class EmbeddedGraphApplication implements ApplicationRunner {

	public static void main(String[] args) {
		SpringApplication.run(EmbeddedGraphApplication.class, args);
	}

	@Override
	public void run(ApplicationArguments args) throws Exception {
		testJanusGraph();
		System.out.println("hello world");
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
