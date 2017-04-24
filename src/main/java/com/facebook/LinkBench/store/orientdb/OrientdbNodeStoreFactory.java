package com.facebook.LinkBench.store.orientdb;

import com.facebook.LinkBench.store.LinkStore;
import com.facebook.LinkBench.store.NodeStore;
import com.facebook.LinkBench.store.NodeStoreFactory;

public class OrientdbNodeStoreFactory implements NodeStoreFactory {
	@Override 
	public NodeStore createNodeStore(LinkStore linkStore)
	{
		if(linkStore ==null )
		{
			return new Orient();
		}
		else 
		{
			return (NodeStore) linkStore;
		}
	}
}
