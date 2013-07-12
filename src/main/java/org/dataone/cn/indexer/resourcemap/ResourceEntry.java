package org.dataone.cn.indexer.resourcemap;

import java.util.Set;

import org.w3c.dom.Element;

public interface ResourceEntry 
{
	//public Element getEntry();
	//public void setEntry(Element entry);
	public Set<String> getResourceMaps();
	public void setResourceMaps(Set<String> resourceMaps);
	public String getIdentifier();
	public void setIdentifier(String identifier);
	public Set<String> getDocuments();
	//public void setDocuments(Set<String> documents);
	public Set<String> getDocumentedBy();
	//public void setDocumentedBy(Set<String> documentedBy);
	//public String getAbout();
	//public void setAbout(String about);
	public String toString();
	public ResourceMap getParentMap();
	//public void setParentMap(ResourceMap parentMap);
}