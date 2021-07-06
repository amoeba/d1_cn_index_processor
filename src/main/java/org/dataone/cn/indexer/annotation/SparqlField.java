package org.dataone.cn.indexer.annotation;

import java.util.List;

import org.dataone.cn.indexer.convert.IConverter;
import org.dataone.cn.indexer.parser.ISolrDataField;
import org.dataone.cn.indexer.solrhttp.SolrElementField;

public class SparqlField implements ISolrDataField {
	
	private String name;
	
	private String query;
	
	private IConverter converter = null;
	
	public SparqlField(String name, String query) {
		this.name = name;
		this.query = query;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	@Override
	public List<SolrElementField> getFields(byte[] data, String arg1)
			throws Exception {
		// TODO Auto-generated method stub
		return null;
	}
	
	/**
     * Get the converter
     * @return the converter associated with this field
     */
    public IConverter getConverter() {
        return converter;
    }

    /**
     * Set the converter
     * @param converter  set the converter to this field
     */
    public void setConverter(IConverter converter) {
        this.converter = converter;
    }

}
