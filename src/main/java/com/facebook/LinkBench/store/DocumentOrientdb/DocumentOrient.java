package com.facebook.LinkBench.store.DocumentOrientdb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import com.facebook.LinkBench.Link;
import com.facebook.LinkBench.Node;
import com.facebook.LinkBench.Phase;
import com.facebook.LinkBench.store.GraphStore;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

public class DocumentOrient extends GraphStore{

	public  ODatabaseDocumentTx db ;
	public  final AtomicLong longGenerator = 
			 new AtomicLong(System.currentTimeMillis());
	
	@Override
	public void resetNodeStore(String dbid, long startID) throws Exception {
		// TODO Auto-generated method stub
		db= new ODatabaseDocumentTx
				("remote:localhost/home/charily/orientdb/databases/linkbench");
		db.open("root", "root");
		OClass Link =db.getMetadata().getSchema().createClass("Link");
		Link.createProperty("link_id1", OType.LONG);
		Link.createProperty("link_id2", OType.LONG);
		Link.createProperty("link_type",OType.LONG);
		Link.createProperty("link_visibility",OType.BYTE);
		Link.createProperty("link_version",OType.INTEGER);
		Link.createProperty("link_time",OType.LONG);
		Link.createProperty("link_data", OType.BINARY);
		Link.createIndex("Linkid1",OClass.INDEX_TYPE.NOTUNIQUE, "link_id1");
		Link.createIndex("Linkid2",OClass.INDEX_TYPE.NOTUNIQUE, "link_id2");
		OClass Node=db.getMetadata().getSchema().createClass("Node");
		Node.createProperty("node_id",OType.LONG);
		Node.createProperty("node_type", OType.INTEGER);
		Node.createProperty("node_version", OType.LONG);
		Node.createProperty("node_time", OType.INTEGER);
		Node.createProperty("node_data", OType.BINARY);
		Node.createProperty("link", OType.LINKLIST, db.getMetadata().getSchema().getClass("Link"));
		Node.createIndex("Node",OClass.INDEX_TYPE.NOTUNIQUE, "node_id");		
		db.getMetadata().getSchema().save();
	}

	@Override
	public long addNode(String dbid, Node node) throws Exception {
		// TODO Auto-generated method stub
		try
		{
			long id =node .id;
			  if(id == -1){
	                id = longGenerator.getAndIncrement();
	            }
			ODocument node1 = new ODocument(db.getMetadata().getSchema().getClass("Node"));
			node1.field("node_id",id);
			node1.field("node_type",node.type);
			node1.field("node_version",node.version);
			node1.field("node_time",node.time);
			node1.field("node_data",node.data);
			
			//Node.createProperty("link", OType.LINKLIST, db.getMetadata().getSchema().getClass("Link"));
			//ODocument node1 = new ODocument(db.getMetadata().getSchema().getClass("Node"));
			
			List<ODocument>list =new ArrayList<>();
			node1.field("link",list);			
			node1.save();
			return node.id;
		}
		catch(OConcurrentModificationException e)
		{
			return 0;
		}
	}

	@Override
	public Node getNode(String dbid, int type, long id) throws Exception {
		// TODO Auto-generated method stub
		try
		{
			
			OSQLSynchQuery <ODocument> query = new OSQLSynchQuery<ODocument>(
					"SELECT FROM NODE WHERE NODE_ID =?");
			List<ODocument> result = db.command(query).execute(id);
			if(result.size() == 0)
			{
				return null;
			}
			else 
			{
				ODocument node = new ODocument
						(db.getMetadata().getSchema().getClass("Node"));
				node =result.get(0);
				long node_id = Long.parseLong(node.field("node_id").toString());
				int  node_type = Integer.parseInt(node.field("node_type").toString()); 
				long node_version = Long.parseLong(node.field("node_version").toString());
				int node_time = Integer.parseInt((node.field("node_time").toString()));
				byte[] node_data = node.field("node_data").toString().getBytes();

				Node node1 = new Node (node_id,node_type,node_version,node_time,node_data);
					return node1;
					
			}
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
			
			OSQLSynchQuery <ODocument> query = new OSQLSynchQuery<ODocument>(
					"SELECT FROM NODE WHERE NODE_ID = ?");
			long id = node.id;
			List<ODocument> result = db.command(query).execute(id);
			if(result.size() == 0)
			{
				return false;
			}
			else 
			{
				result.get(0).field("node_type", node.type);
				result.get(0).field("node_version", node.version);
				result.get(0).field("node_time",node.time);
				result.get(0).field("node_data",node.data);
				result.get(0).save();
				return true;
			}
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
			
			int modified = db.
				command(new OCommandSQL(
				"delete from Node where node_id ="+id)).execute();
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
		db = new ODatabaseDocumentTx
				("remote:localhost/home/charily/orientdb/databases/linkbench");
		db.open("root", "root");
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
		try
		{
			
			long id1 = a.id1;
			long id2 = a.id2;
			ODocument link = new ODocument((db.getMetadata().getSchema().getClass("Link")));
			link.field("link_id1", a.id1);
			link.field("link_id2", a.id2);
			link.field("link_type", a.link_type);
			link.field("link_visibility", a.visibility);
			link.field("link_version",a.version);
			link.field("link_time", a.time);
			link.field("link_data",a.data);
			link.save();
			OSQLSynchQuery <ODocument> query = new OSQLSynchQuery<ODocument>(
					"SELECT FROM NODE WHERE NODE_ID =?");
			List<ODocument> result1 = db.command(query).execute(id1);
			if(result1.size()== 0)
			{
			//	System.out.println("id1 null");
				ODocument node1 = new ODocument(db.getMetadata().getSchema().getClass("Node"));
				node1.field("node_id",id1);
				List<ODocument>list=new ArrayList<>();
				list.add(link);
				node1.field("link", list);
				node1.save();
			}
			else 
			{
			//	System.out.println("id1 exist");
				List<ODocument>list1 = new ArrayList<>();
				list1 = result1.get(0).field("link");//System.out.println("!!!"+list1);
				list1 . add(link);//System.out.println("@@@"+list1);
				result1.get(0).field("link", list1);
				result1.get(0).save();
			}
			List<ODocument> result2 = db.command(query).execute(id2);
			if(result2.size()== 0)
			{
			//	System.out.println("id2 null");
				ODocument node2 = new ODocument(db.getMetadata().getSchema().getClass("Node"));
				node2.field("node_id",id2);
				List<ODocument>list = new ArrayList<>();
				list.add(link);
				node2.field("link",list);
				node2.save();
			}
			else 
			{
			//	System.out.println("id2 exist");
				List<ODocument>list2 = new ArrayList<>();
				list2 = result2.get(0).field("link");//System.out.println(list2);
				list2 .add(link);//System.out.println(list2);
				result2.get(0).field("link", list2);
				result2.get(0).save();
			}		
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
		// TODO Auto-generated method stub
		try
		{
			
			int modified = db.
					command(new OCommandSQL(
							"delete from Link where link_id1 ="+id1+
							" and link_id2 ="+id2)).execute();
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
			
			deleteLink(dbid , a.id1 , 0 , a.id2 , true , true);
			addLink(dbid,a,noinverse);
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
		
			OSQLSynchQuery <ODocument> query = new OSQLSynchQuery<ODocument>(
					"SELECT FROM Link WHERE link_id1 = ? and link_id2 = ?");
			List<ODocument> result = db.command(query).execute(id1,id2);
			if(result.size() == 0)
			{
		//		System.out.println("getlink null");
				return null;
			}
			else 
			{
		//		System.out.println("found");
				Link link = new Link();
				link.id1 = Long.parseLong(result.get(0).field("link_id1").toString());
				link.id2 = Long.parseLong(result.get(0).field("link_id2").toString());
				link.link_type = Long.parseLong(result.get(0).field("link_type").toString());
				link.data = result.get(0).field("link_data").toString().getBytes();
				return link;
			}	
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
		catch(OConcurrentModificationException  e)
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
			
			OSQLSynchQuery <ODocument> query = new OSQLSynchQuery<ODocument>(
					"SELECT FROM Link WHERE link_id1 = ? ");
			List<ODocument> result = db.command(query).execute(id1);
			if(result.size() == 0)
			{
				return null;
			}
			else 
			{
				ArrayList <Link> links = new ArrayList<Link>();
				for(int i=0;i<result.size();i++)
				{
					ODocument  ODocument_link = new ODocument
							(db.getMetadata().getSchema().getClass("Link"));
					ODocument_link = result.get(i);
					Link link = new Link();
					link.id1 = Long.parseLong( ODocument_link .field("link_id1").toString());
					link.id2 = Long.parseLong( ODocument_link .field("link_id2").toString());
					link.link_type=Long.parseLong(ODocument_link.field("link_type").toString());
					link.time=Long.parseLong(ODocument_link.field("link_time").toString());
					link.data = ODocument_link.field("link_data").toString().getBytes();
					links.add(link);
				}
					List<Link> subList = links.subList
							(offset, Math.min(result.size(), offset + limit));	
					return subList.toArray(new Link[subList.size()]);
			}
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
			
			OSQLSynchQuery <ODocument> query = new OSQLSynchQuery<ODocument>(
					"SELECT FROM Link WHERE link_id1 = ? ");
			List<ODocument> result = db.command(query).execute(id1);
			return result.size();
		}
		catch(OConcurrentModificationException e)
		{
			return 0;
		}
	}

}
