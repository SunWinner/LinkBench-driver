package com.facebook.LinkBench.store.DocumentOrientdb;

import com.facebook.LinkBench.store.LinkStore;
import com.facebook.LinkBench.store.NodeStore;
import com.facebook.LinkBench.store.NodeStoreFactory;

public class DocumentOrientdbNodeStoreFactory  implements NodeStoreFactory{

	@Override
	public NodeStore createNodeStore(LinkStore linkStore) {
		
		if(linkStore == null )
		{
			return new DocumentOrient();
		}
		else 
		{
			return (NodeStore) linkStore;
		}
	}
	
}
