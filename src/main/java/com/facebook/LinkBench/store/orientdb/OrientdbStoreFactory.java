package com.facebook.LinkBench.store.orientdb;

import com.facebook.LinkBench.store.LinkStore;
import com.facebook.LinkBench.store.LinkStoreFactory;

public class OrientdbStoreFactory  implements LinkStoreFactory{
	@Override 
	public LinkStore createLinkStore()
	{
		return  new Orient();
	}
}
