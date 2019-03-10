import java.util.List;

import org.jgrapht.alg.interfaces.ShortestPathAlgorithm.SingleSourcePaths;
import org.jgrapht.alg.interfaces.StrongConnectivityAlgorithm;
import org.jgrapht.alg.shortestpath.DijkstraShortestPath;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.Graph;

/**
 * This class demonstrates some of the operations that can be performed on directed graphs. After
 * constructing a basic directed graph, it computes all the strongly connected components of this
 * graph. It then finds the shortest path from one vertex to another using Dijkstra's shortest path
 * algorithm. The sample code should help to clarify to users of JGraphT that the class
 * org.jgrapht.alg.shortestpath.DijkstraShortestPath can be used to find shortest paths within
 * directed graphs.
 *
 * @author  PurnachandraRao
 * @since 2017-03-17
 */
 
 public class FullyConnected
{
    public static void main(String args[])
    {
        // constructs a directed graph with the specified vertices and edges
        DefaultDirectedGraph<String, DefaultEdge> directedGraph =
            new DefaultDirectedGraph<String, DefaultEdge>(DefaultEdge.class);
			for (i=1; i<=n; i++)
				directedGraph.addVertex(i); // end of vertices addition
			for (i=1; i<=n; i++){
			 for (j=1; j<=n; j++){
				if(i==j)
				CONTINUE;
				directedGraph.addEdge(i, j); //end of edges addition to form a graph
					
				}
			} //end of graph creation
			
		// Prints the shortest path from vertex i to vertex c. This certainly
        // exists for our particular directed graph.
        System.out.println("Shortest path from i to c:");
        DijkstraShortestPath<String, DefaultEdge> dijkstraAlg =
            new DijkstraShortestPath<>(directedGraph);
        SingleSourcePaths<String, DefaultEdge> iPaths = dijkstraAlg.getPaths("i");
        System.out.println(iPaths.getPath("c") + "\n");

        // Prints the shortest path from vertex c to vertex i. This path does
        // NOT exist for our particular directed graph. Hence the path is
        // empty and the variable "path"; must be null.
        System.out.println("Shortest path from c to i:");
        SingleSourcePaths<String, DefaultEdge> cPaths = dijkstraAlg.getPaths("c");
        System.out.println(cPaths.getPath("i"));
    }
}

