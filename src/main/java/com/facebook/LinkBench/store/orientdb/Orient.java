package com.facebook.LinkBench.store.orientdb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import com.facebook.LinkBench.Link;
import com.facebook.LinkBench.Node;
import com.facebook.LinkBench.Phase;
import com.facebook.LinkBench.store.GraphStore;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Parameter;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientEdgeType;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;

public class Orient extends GraphStore{
	
	public static final String Link_id1="link_id1";
	public static final String Link_id2="link_id2";
	public static final String Link_type="link_type";
	public static final String Link_time="link_time";
	public static final String Link_version="link_version";
	public static final String Link_visibility="link_visibility";
	public static final String Link_data="link_data";
	
	
	public static final String Node_id="node_id";
	public static final String Node_time="node_time";
	public static final String Node_type="node_type";
	public static final String Node_version="node_version";
	public static final String Node_data="node_data";
	
	OrientGraphFactory factory;
	OrientGraph orientGraph;
	static final AtomicLong longGenerator = new AtomicLong(System.currentTimeMillis());

	@Override
	public void resetNodeStore(String dbid, long startID) throws Exception {
		// TODO Auto-generated method stub
		
		//System.out.println("Orient:::::::resetNodeStore");
		
		factory = new OrientGraphFactory("remote:/home/charily/orientdb/databases/linkbench").setupPool(1, 500);
		orientGraph =factory.getTx();
		
		OrientVertexType Node = orientGraph.createVertexType("Node");
		Node.createProperty(Node_id  , OType.LONG);
		Node.createProperty(Node_type, OType.INTEGER);
		Node.createProperty(Node_version, OType.LONG);
		Node.createProperty(Node_time, OType.INTEGER);
		Node.createProperty(Node_data, OType.BINARY);
		orientGraph.createKeyIndex(Node_id, Vertex.class,new Parameter("class", "Node"));
		//graph.createKeyIndex("name", Vertex.class, new Parameter("class", "Customer"));
		OrientEdgeType Link = orientGraph.createEdgeType("Link");
		Link.createProperty(Link_id1, OType.LONG);
		Link.createProperty(Link_id2, OType.LONG);
		Link.createProperty(Link_type,OType.LONG);
		Link.createProperty(Link_visibility,OType.BYTE);
		Link.createProperty(Link_version,OType.INTEGER);
		Link.createProperty(Link_time,OType.LONG);
		Link.createProperty(Link_data, OType.BINARY);
		orientGraph.createKeyIndex(Link_id1, Edge.class,new Parameter("class", "Link"));
		orientGraph.createKeyIndex(Link_id2, Edge.class,new Parameter("class", "LInk"));
	}

	@Override
	public long addNode(String dbid, Node node) throws Exception {
		// TODO Auto-generated method stub
		//System.out.print("Orient::::::::::::addNode");
		try
		{
			long startTime = System.currentTimeMillis(); 
			Vertex vertex = orientGraph.addVertex("class:Node");
			vertex.setProperty(Node_id, node.id);
			vertex.setProperty(Node_type, node.type);
			vertex.setProperty(Node_version, node.version);
			vertex.setProperty(Node_time, node.time);
			vertex.setProperty(Node_data,  node.data);
			orientGraph.commit();
			long endTime = System.currentTimeMillis(); 
		//	System.out.println("addNode程序运行时间：" + (endTime - startTime) + "ms");
			return node.id;
			
		}
		catch(OConcurrentModificationException e)
		//
		{		
			
		}
		return 0;
	}
	
	public Vertex getVertex(long id) throws Exception
	{	
		try
		{
		//	System.out.print("getVertex");
			Iterator <Vertex> vertexes = orientGraph.getVertices("Node.node_id",Long.toString(id)).iterator();
			if(vertexes.hasNext())
			{
				return null;
			}
			else 
			{
				Vertex v1 = vertexes.next();
				return v1;
			}	
		}
		catch(OConcurrentModificationException e)
		{
			return null;
		}
				
	}
	
	@Override
	public Node getNode(String dbid, int type, long id) throws Exception {
		// TODO Auto-generated method stub
		try
		{
			long startTime = System.currentTimeMillis(); 
		//	System.out.print("getNode");			
			Vertex vertex = getVertex(id);
			if(vertex == null)
			{
	//			System.out.println("getnode not found");
				return null;
			}
			long vertexid      = vertex.getProperty(Node_id);
			int vertexType     = vertex.getProperty(Node_type);
			long vertexVersion = vertex.getProperty(Node_version);
			int vertexTime     = vertex.getProperty(Node_time);
			byte[] vertexData  = vertex.getProperty(Node_data);
			
			/* Node(long id, int type, long version, int time,
		       byte data[])
		       */
			Node node = new Node (vertexid,vertexType,vertexVersion,vertexTime,vertexData);
			orientGraph.commit();
			long endTime = System.currentTimeMillis(); 
		//	System.out.println("getNode程序运行时间：" + (endTime - startTime) + "ms"); 
			return node;
		}
		catch(OConcurrentModificationException e)
		{
			return null;
		}
	}

	@Override
	public boolean updateNode(String dbid, Node node) throws Exception {
		// TODO Auto-generated method stub
		try
		{
			long startTime = System.currentTimeMillis(); 
		//	System.out.print("updateNode");	
			Vertex vertex =getVertex(node.id);
			if(vertex == null)
			{
				return false;
			}
			vertex.setProperty(Node_time, node.time);
			vertex.setProperty(Node_type, node.type);
			vertex.setProperty(Node_version, node.version);
			vertex.setProperty(Node_data, node.data);
			orientGraph.commit();
			long endTime = System.currentTimeMillis();  
		//	System.out.println("updataNode程序运行时间：" + (endTime - startTime) + "ms"); 
			return true;
		}
		catch(OConcurrentModificationException e)
		{
			return false;
		}
		
	}

	@Override
	public boolean deleteNode(String dbid, int type, long id) throws Exception {
		// TODO Auto-generated method stub
		
		try
		{
			long startTime = System.currentTimeMillis();
		//	System.out.print("deleteNode");
			Vertex vertex = getVertex(id);
			if(vertex ==  null)
			{
				return false;
			}
			orientGraph.removeVertex(vertex);
			orientGraph.commit();
			long endTime = System.currentTimeMillis();
		//	System.out.println("deleteNode程序运行时间：" + (endTime - startTime) + "ms");  
			return true;
			
		}
		catch(OConcurrentModificationException e)
		{
			return false;
		}
	
	}

	@Override
	public void initialize(Properties p, Phase currentPhase, int threadId) throws IOException, Exception {
		// TODO Auto-generated method stub
	//	System.out.println("initialize");
		factory = new OrientGraphFactory("remote:/home/charily/orientdb/databases/linkbench").setupPool(1, 500);
		orientGraph =factory.getTx();
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
	//	orientGraph.shutdown();
	//	factory.close();
	}

	@Override
	public void clearErrors(int threadID) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean addLink(String dbid, Link a, boolean noinverse) throws Exception {
		
	/*//	System.out.println("addlink");
		long id1 = a.id1;
		long id2 = a.id2;
	//	System.out.println("id1"+id1+"id2"+id2);
		Vertex vertex1 = orientGraph.addVertex("class:Node");
		vertex1.setProperty(Node_id, id1);
		Vertex vertex2 = orientGraph.addVertex("class:Node");
		vertex2.setProperty(Node_id, id2);
		 Edge edge = orientGraph.
					addEdge(null, vertex1, vertex2, orientGraph.getEdgeType("Link").getName());
		 edge.setProperty(Link_id1, a.id1);
	     edge.setProperty(Link_id2, a.id2);
	     edge.setProperty(Link_type, 123456789);
	     edge.setProperty(Link_visibility, a.visibility);
	     edge.setProperty(Link_data, a.data);
	     edge.setProperty(Link_version, a.version);
	     edge.setProperty(Link_time, a.time);
	     orientGraph.commit();
	     return true;*/		
		// TODO Auto-generated method stub
	try
	{
		long startTime = System.currentTimeMillis(); 
		long id1 = a.id1;
		long id2 = a.id2;
	//	System.out.print("addlink");	
		Iterator <Vertex> vertexes1 = 
				orientGraph.getVertices("Node.node_id",Long.toString(id1)).iterator();	
		Iterator <Vertex> vertexes2 = 
				orientGraph.getVertices("Node.node_id",Long.toString(id2)).iterator();
		
	//	Vertex vertex1 = orientGraph.addVertex("class:Node");
	//	Vertex vertex2 = orientGraph.addVertex("class:Node");
		
		if (!vertexes1.hasNext()){
	         Vertex vertex3 = orientGraph.addVertex("class:Node");
	         vertex3.setProperty("node_id", id1);
	//         vertex1 = vertex3;
	        }
	//	else 
	//	{
	//		vertex1=vertexes1.next();
	//	}
		if(!vertexes2.hasNext())
		{   
			 Vertex vertex4 = orientGraph.addVertex("class:Node");		 
			 vertex4.setProperty("node_id", id2);
	//		 vertex2=vertex4;
		}
	//	else 
	//	{
	//		vertex2=vertexes2.next();
	//	}
		Iterator <Vertex> vertexes3 = 
				orientGraph.getVertices("Node.node_id",Long.toString(id1)).iterator();
		Iterator <Vertex> vertexes4 = 
				orientGraph.getVertices("Node.node_id",Long.toString(id2)).iterator();
		Vertex v3= vertexes3.next();
		Vertex v4= vertexes4.next();
	//	 Edge edge = orientGraph.
	//					addEdge(null, vertex1, vertex2, orientGraph.getEdgeType("Link").getName());
			 Edge edge = orientGraph.
						addEdge(null,v3, v4, orientGraph.getEdgeType("Link").getName());
			 edge.setProperty(Link_id1, a.id1);
		     edge.setProperty(Link_id2, a.id2);
		     edge.setProperty(Link_type, 123456789);
		     edge.setProperty(Link_visibility, a.visibility);
		     edge.setProperty(Link_data, a.data);
		     edge.setProperty(Link_version, a.version);
		     edge.setProperty(Link_time, a.time);
		     orientGraph.commit();
		     long endTime = System.currentTimeMillis();
	//	     System.out.println("AddLink程序运行时间：" + (endTime - startTime) + "ms"); 
		     return true;
	}
	catch(OConcurrentModificationException e)
	{
		return false;
	}
		
				     	
	}
	@Override
	public boolean deleteLink(String dbid, long id1, long link_type, long id2, boolean noinverse, boolean expunge)
			throws Exception {
		try
		{
	//		System.out.print("deletelink");
			long startTime = System.currentTimeMillis(); 
			int modified = orientGraph.
					command(new OCommandSQL(
							"delete edge where link_id1 ="+id1+
							" and link_id2 ="+id2)).execute();
			long endTime = System.currentTimeMillis(); 
	//		System.out.println("deleteLInk程序运行时间：" + (endTime - startTime) + "ms"); 
			return true;
		}
		catch(OConcurrentModificationException e)
		{
			return false;
		}

		
	}

	@Override
	public boolean updateLink(String dbid, Link a, boolean noinverse) throws Exception {
		// TODO Auto-generated method stub
		
		try
		{
	//		System.out.print("updateLInk");
			long startTime = System.currentTimeMillis(); 
			long id1=a.id1;
			long id2=a.id2;
			long link_type=0;	
			deleteLink(dbid,id1,0,id2,true,true);
			addLink(dbid,a,noinverse);
					//addLink(String dbid, Link a, boolean noinverse)
			long endTime = System.currentTimeMillis(); 
	//		System.out.println("updateLink程序运行时间：" + (endTime - startTime) + "ms");  
			return true;
		}
		catch(OConcurrentModificationException e)
		{
			return false;
		}
		
	}

	@Override
	public Link getLink(String dbid, long id1, long link_type, long id2) throws Exception {
		// TODO Auto-generated method stub
		try
		{
	//		System.out.print("getLink");
			Iterator <Edge> edges = orientGraph.getEdges("Link.link_id2", Long.toString(id2)).iterator();
			while(edges.hasNext())
			{
				if((long)edges.next().getProperty("link_id1")== id1)
				{
					Link link =new Link();
					link.id1=id1;
					link.id2=id2;
					link.link_type=link_type;
					
					return link;
				}
			}
			return null;
		}
		catch(OConcurrentModificationException e)
		{
			return null;
		}
		
	}

	@Override
	public Link[] getLinkList(String dbid, long id1, long link_type) throws Exception {
		// TODO Auto-generated method stub
		try
		{
			
			return getLinkList(dbid ,id1 ,link_type ,0, Long.MAX_VALUE ,0 ,10000);
		}
		catch(OConcurrentModificationException e)
		{
			return null;
		}
		
	}

	@Override
	public Link[] getLinkList(String dbid, long id1, long link_type, long minTimestamp, long maxTimestamp, int offset,
			int limit) throws Exception {
		// TODO Auto-generated method stub
		
		try
		{
	//		System.out.print("getLinkList");
			ArrayList <Link> result =new ArrayList<Link>();
			Vertex vertex =getVertex(id1);
			if(vertex ==null)
			{
				return null;
			}
			Iterator <Edge> edges = orientGraph.
					getEdges("Link.link_id1", Long.toString(id1))
							.iterator();
			while(edges.hasNext())
			{
				long timestamp=edges.next().getProperty("link_time");
				if(timestamp < minTimestamp || timestamp > maxTimestamp)
				{
					continue;
				}
				Edge edge =edges.next();
				Link link =new Link();
				link.id1=id1;
				link.id2=edge.getProperty("link_id2");
				link.link_type = link_type;
				link.time = timestamp;
				link.version = edge.getProperty("link_version");
				link.visibility=edge.getProperty("link_visibility");
				link.data=edge.getProperty("link_data");
				result.add(link);
			}
			Collections.sort(result, new Comparator<Link>() {
	            @Override
	            public int compare(Link link, Link link2) {
	                if (link2.time > link.time) return 1;
	                if (link2.time == link.time) return 0;
	                return -1;
	            }
	        });
		  if (result.isEmpty()) {
	            return null;
	        }
		  List<Link> subList = result.subList(offset, Math.min(result.size(), offset + limit));
		  return subList.toArray(new Link[subList.size()]);
		}
		catch(OConcurrentModificationException e)
		{
			return null;
		}
		
	}

	@Override
	public long countLinks(String dbid, long id1, long link_type) throws Exception {
		// TODO Auto-generated method stub
		try
		{	
	//		System.out.print("countlinks");
			Iterator <Edge> edges = orientGraph.
					getEdges("Link.link_id1", Long.toString(id1))
							.iterator();
			int count=0;
			while(edges.hasNext())
			{
				count++;
				edges.next();
			}
			
			return count;
		}
		catch(OConcurrentModificationException e)
		{
			return 0;
		}
		
	}

}
