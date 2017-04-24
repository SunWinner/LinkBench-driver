package com.facebook.LinkBench.store.orientdb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import com.facebook.LinkBench.Link;
import com.facebook.LinkBench.LinkCount;
import com.facebook.LinkBench.Node;
import com.facebook.LinkBench.Phase;
import com.facebook.LinkBench.store.GraphStore;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Parameter;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientEdgeType;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;
import com.tinkerpop.blueprints.util.wrappers.id.IdGraph;

public class OrientdbStore extends GraphStore {

		/*
		 * 为了使用Graph API，您需要创建一个OrientGraph类的实例。
		 * 构造函数接收数据库URL，该URL是数据库的位置。
		 * 如果数据库已经存在，则图形API将打开它。如果它不存在，
		 * 则Graph API会创建它。
		 */	
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
	
	OrientGraph txGraph;
	
//	static VertexCache cache;
	
	static final AtomicLong longGenerator = new AtomicLong(System.currentTimeMillis());
	
	static HashMap<Long,Long> count = new HashMap<Long,Long>();
	/*
	 * Create Custom Vertex Classes
	 * OrientVertexType accountVertex 
	 * 							= graph.createVertexType("Account");
	 * Create Custom Edge Class
	 * OrientEdgeType livesedge = graph.createEdgeType("Lives");
	 * 
	 * Create Vertices
	 * Vertex account = graph.addVertex("class:Account");
	 * 
	 * Create Edge 
	 * dge e = account.addEdge("Lives", address);
	 */	
	@Override
	public void resetNodeStore(String dbid, long startID) throws Exception {
		// TODO Auto-generated method stub
		System.out.println("resetNodeStore");		
		factory = new OrientGraphFactory("remote:/home/charily/orientdb/databases/linkbench").setupPool(1, 500);
		
		OrientVertexType Node = txGraph.createVertexType("Node");
		Node.createProperty(Node_id  , OType.LONG);
	//	Node.createKeyIndex("id", Vertex.class, new Parameter("type", "UNIQUE"));
		Node.createProperty(Node_type, OType.INTEGER);
		Node.createProperty(Node_version, OType.LONG);
		Node.createProperty(Node_time, OType.INTEGER);
		Node.createProperty(Node_data, OType.BINARY);
		/*
		public static final String Node_id="node_id";
		public static final String Node_time="node_time";
		public static final String Node_type="node_type";
		public static final String Node_version="node_version";
		public static final String Node_data="node_data";*/
		
		OrientEdgeType Link = txGraph.createEdgeType("Link");
		Link.createProperty(Link_id1, OType.LONG);
		Link.createProperty(Link_id2, OType.LONG);
		Link.createProperty(Link_type,OType.LONG);
		Link.createProperty(Link_visibility,OType.BYTE);
		Link.createProperty(Link_version,OType.INTEGER);
		Link.createProperty(Link_time,OType.LONG);
		Link.createProperty(Link_data, OType.BINARY);	
		/*
		public static final String Link_id1="link_id1";
		public static final String Link_id2="link_id2";
		public static final String Link_type="link_type";
		public static final String Link_time="link_time";
		public static final String Link_version="link_version";
		public static final String Link_visibility="link_visibility";
		public static final String Link_data="link_data";*/			
		//cache = VertexIDType.NUMBER.getVertexCache();
		txGraph =factory.getTx();
		//
	}

	@Override
	public long addNode(String dbid, Node node) throws Exception {
		// TODO Auto-generated method stub
		System.out.println("addNode");
		try
		{
			long id=node.id;
			if(id== -1)
			{
				id=longGenerator.getAndIncrement();
			}
			Vertex vertex =txGraph.addVertex("class:Node");
			vertex.setProperty(Node_id, id);
			//vertex.setProperty(IdGraph.ID , id );
			vertex.setProperty(Node_type,node.type);
			vertex.setProperty(Node_version,node.version);
			vertex.setProperty(Node_time, node.time);
			vertex.setProperty(Node_data, node.data);
			txGraph.commit();
			return id;
		}
		catch(Exception e)
		{
			txGraph.rollback();
			throw e;
		}
	}
	
/*	@Override 
	public long[] bulkAddNodes(String dbid, List<Node>nodes)throws Exception
	{
		System.out.println("bulkAddNodes");
	//	long time=System.nanoTime();
		final long[] result =new long[nodes.size()];
			
		for(int i=0;i<nodes.size();i++)
		{
			Node node =nodes.get(i);
			Vertex vertex =txGraph.addVertex("class:Node");
			//OrientVertexType Node = txGraph.createVertexType("Node");
			vertex.setProperty(Node_id, node.id);
			vertex.setProperty(Node_type,node.type);
			vertex.setProperty(Node_version,node.version);
			vertex.setProperty(Node_time, node.time);
			vertex.setProperty(Node_data, node.data);
			result[i]=node.id;
//			cache.set(vertex, node.id);
// static VertexCache cache;	
			txGraph.commit();
		}
		
		System.out.println("for  over");
	//	long elapsedTime = System.nanoTime() - time ;
     //   long seconds = elapsedTime / 1000000000;
       // System.out.print("["+nodes.get(0).id+ "]"+seconds+"s ");
//        cache.newTransaction();
        return result;
	}
*/

	@Override
	public Node getNode(String dbid, int type, long id) throws Exception {
		// TODO Auto-generated method stub
		Vertex vertex = getVertex(id);
		if(vertex == null)
		{
			return null;
		}
		long vertexid      = (Long)vertex.getProperty(Node_id);
		int vertexType     = vertex.getProperty(Node_type);
		long vertexVersion = vertex.getProperty(Node_version);
		int vertexTime     = vertex.getProperty(Node_time);
		byte[] vertexData  = vertex.getProperty(Node_data);
		Node node =new Node(vertexid,vertexType,vertexVersion,vertexTime,vertexData);
		return node;
		//
	}
	
	public Vertex getVertex(long id)
	{
		Iterator<Vertex> vertexes =txGraph.getVertices(Node_id, id).iterator();
		if(!vertexes.hasNext())
		{
			return null;
		}
		return vertexes.next();
		//
	}

	@Override
	public boolean updateNode(String dbid, Node node) throws Exception {
		// TODO Auto-generated method stub
		Vertex vertex =getVertex(node.id);
		if(vertex == null)
		{
			return false;
		}
		vertex.setProperty(Node_time, node.time);
		vertex.setProperty(Node_type, node.type);
		vertex.setProperty(Node_version, node.version);
		vertex.setProperty(Node_data, node.data);
		return true;
		//
	}

	@Override
	public boolean deleteNode(String dbid, int type, long id) throws Exception {
		// TODO Auto-generated method stub
		try
		{
			Vertex vertex = getVertex(id);
			if(vertex ==null)
			{
				return false;
			}
			for(Edge edge:getVertex(id).getEdges(Direction.BOTH))
			{
				edge.remove();
			}
			txGraph.removeVertex(vertex);
			txGraph.commit();
			return true;
		}
		catch(Exception exception)
		{
			txGraph.rollback();
			return false;
		}
	}

	@Override
	public void initialize(Properties p, Phase currentPhase, int threadId) throws IOException, Exception {
		// TODO Auto-generated method stub
	//	factory = new OrientGraphFactory("plocal:/home/charily/demo","admin","admin").setupPool(1, 100);
		System.out.println("initialize");
		/*factory = new OrientGraphFactory("remote:/home/charily/orientdb/databases/linkbench").setupPool(1, 100);	
		txGraph =factory.getTx();*/
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void clearErrors(int threadID) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean addLink(String dbid, Link a, boolean noinverse) throws Exception {
		// TODO Auto-generated method stub
		List<Edge>edges =new ArrayList<Edge>();
		if(getVertex(a.id1) == null)
		{
			System.out.println("addLink+id1=null");
			addNode(dbid,new Node(a.id1,DEFAULT_NODE_TYPE,0,0,new byte[0]));
		}
		if(getVertex(a.id2) == null)
		{
			System.out.println("addLink+id2=null");
			addNode(dbid,new Node(a.id2,DEFAULT_NODE_TYPE,0,0,new byte[0]));
		}
		for(Edge edge :getVertex(a.id1).getEdges(Direction.OUT, Long.toString(a.link_type)))
		{
			Long id =edge.getVertex(Direction.IN).getProperty(Node_id);
			if(id!=null && id.equals(a.id2))
			{
				edges.add(edge);
			}
		}
		if(edges.isEmpty())
		{
			Edge edge =getVertex(a.id1).addEdge(Long.toString(a.link_type), getVertex(a.id2));
			edge.setProperty(Link_visibility, a.visibility);
			edge.setProperty(Link_data, a.data);
			edge.setProperty(Link_version, a.version);
			edge.setProperty(Link_time, a.time);
			txGraph.commit();
			return true;
		}
		else
		{
			Edge edge =edges.iterator().next();
			edge.setProperty(Link_visibility, a.visibility);
			edge.setProperty(Link_data, a.data);
			edge.setProperty(Link_version, a.version);
			edge.setProperty(Link_time, a.time);
			txGraph.commit();
			return false;
		}
		
	}

	@Override
	public boolean deleteLink(String dbid, long id1, long link_type, long id2, boolean noinverse, boolean expunge)
			throws Exception {
		// TODO Auto-generated method stub
		Edge edge = getEdge(id1, link_type, id2);
		if(edge != null)
		{
			if(expunge)
			{
				edge.remove();
				txGraph.commit();
			}
			else 
			{
				edge.setProperty(Link_type, VISIBILITY_HIDDEN);
			}
			return true;
		}
		return false;
	}
	public Edge getEdge(long id1,long link_type,long id2)
	{
		Vertex vertex1 = getVertex(id1);
		Vertex vertex2 = getVertex(id2);
		if(vertex1 == null || vertex2 == null)
		{
			return null;
		}
		Edge resultEdge = null;
		for(Edge edge :vertex1.getEdges(Direction.OUT,Long.toString(link_type)))
		{
			Long id = edge.getVertex(Direction.IN).getProperty(IdGraph.ID);
            if(id!=null && id.equals(vertex2.getProperty(IdGraph.ID))) {
                resultEdge = edge;
            }
		}
	return resultEdge;
	}

	@Override
	public boolean updateLink(String dbid, Link a, boolean noinverse) throws Exception {
		// TODO Auto-generated method stub
		return !addLink(dbid, a, noinverse);
		//return false;
	}

	@Override
	public Link getLink(String dbid, long id1, long link_type, long id2) throws Exception {
		// TODO Auto-generated method stub
		Edge edge = getEdge(id1, link_type, id2);
		if(edge != null )
		{
			Link link = edgeToLink(edge);
			return link;
		}
		return null;
	}
	public static Link edgeToLink(Edge edge)
	{
		try
		{
			Long id =edge.getVertex(Direction.IN).getProperty(Node_id);
			if(id == null)
			{
				return null;
			}
			return new Link(id,
							Long.parseLong(edge.getLabel()),
							(Long)edge.getVertex(Direction.OUT).getProperty(Node_id),
							(Byte)edge.getProperty(Link_visibility),
							(byte[])edge.getProperty(Node_data),
							(Integer)edge.getProperty(Link_version),
							(Long)edge.getProperty(Link_time));
			/*  Link(long id1, long link_type, long id2,
      			byte visibility, byte[] data, int version, long time)
			 */
		}
		catch(Exception e)
		{
			return null;
		}
	}
	

	@Override
	public Link[] getLinkList(String dbid, long id1, long link_type) throws Exception {
		// TODO Auto-generated method stub
		//return null;
		 return getLinkList(dbid, id1, link_type, 0, Long.MAX_VALUE, 0, rangeLimit);
	}

	@Override
	public Link[] getLinkList(String dbid, long id1, long link_type, long minTimestamp, long maxTimestamp, int offset,
			int limit) throws Exception {
		// TODO Auto-generated method stub
		ArrayList<Link>result =new ArrayList<Link>();
		Vertex vertex =getVertex(id1);
		if(vertex == null)
		{
			return null;
		}
		Iterable<Edge>edges =
				vertex.getEdges(Direction.OUT, Long.toString(link_type));
		for(Edge edge :edges)
		{
			if(VISIBILITY_HIDDEN == (Byte) edge.getProperty(Link_visibility))
			{
				continue;
			}
			long timestamp = edge.getProperty(Link_time);
			if (timestamp < minTimestamp || timestamp > maxTimestamp) {
                continue;
            }
			 Link link = edgeToLink(edge);
	            if (link!=null)
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

	@Override
	public long countLinks(String dbid, long id1, long link_type) throws Exception {
		// TODO Auto-generated method stub
		Vertex vertex = getVertex(id1);
		int count = 0;
        if (vertex != null) {
            Iterable<Edge> edges = vertex.getEdges(Direction.OUT, String.valueOf(link_type));
            for(Edge edge : edges) {
                count++;
            }
        }
        return count;
	}
	public int bulkLoadBatchSize()
	{
		return 16384;
	}
	@Override
	public void addBulkLinks
			(String dbid, final List<Link> links, boolean noinverse)
	{
		System.out.println("addBulkLinks");
		try 
		{
            Long firstId = links.get(0).id1;
            if (firstId == links.get(links.size()-1).id1) {
                Long l = count.get(firstId);
                if (l==null) {
                    l=0l;
                }
                l+=links.size();
                count.put(firstId,l);
                if (l>100000) {
                    System.out.println(firstId);
                    return;
                }
            }
//            cache.newTransaction();
            for (Link link : links) 
            {   	 
            	try
            	{
            		/*final Vertex v1 = retrieveFromCache(link.id1);
                    final Vertex v2 = retrieveFromCache(link.id2);*/
            		Vertex v1 = getVertex(link.id1);
            		Vertex v2 = getVertex(link.id2);
                    Edge edge = txGraph.addEdge(null, v1, v2, txGraph.getEdgeType("Link").getName());
               //     Edge edge = txGraph.addEdge(null, v1, v2,Long.toString(link.link_type));
                    edge.setProperty(Link_id1, v1.getProperty(Node_id));
                    edge.setProperty(Link_id2, v2.getProperty(Node_id));
                    //link_type  123456789
                    edge.setProperty(Link_type, 123456789);
                    edge.setProperty(Link_visibility, link.visibility);
                    edge.setProperty(Link_data, link.data);
                    edge.setProperty(Link_version, link.version);
                    edge.setProperty(Link_time, link.time);
            	}
            	catch(Exception e)
            	{
            		e.printStackTrace();
            	}
            	txGraph.commit();
            }
         }
		catch(Exception e)
		{
			
		}
	}
	
/*	public Vertex retrieveFromCache(final Object externalID)
	{
		Object internal = cache.getEntry(externalID);
        if (internal instanceof Vertex) {
            return (Vertex) internal;
        } else if (internal != null) { //its an internal id
            Vertex v = txGraph.getVertex(internal);
            cache.set(v, externalID);
            return v;
        } 
        else return null;
	}*/
	@Override
	public void addBulkCounts(String dbid, List<LinkCount> a) throws Exception {
	        //do nothing, links counted on the fly
	 }
		
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	

}
