package com.facebook.LinkBench.store.DocumentOrientdb;

import com.facebook.LinkBench.store.LinkStore;
import com.facebook.LinkBench.store.LinkStoreFactory;

public class DocumentOrientdbStoreFactory implements LinkStoreFactory{

	@Override
	public LinkStore createLinkStore() {
		
		return  new DocumentOrient();
	}

}
