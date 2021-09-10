package org.dataone.cn.indexer.annotation;

import java.util.List;

import org.dataone.cn.indexer.convert.IConverter;
import org.dataone.cn.indexer.parser.ISolrDataField;
import org.dataone.cn.indexer.solrhttp.SolrElementField;

public class SparqlField implements ISolrDataField {
	
	private String name;
	
	private String query;
	
	private IConverter converter = null;
	Boolean concatValues = false;
	protected String separator = null;
	protected boolean multivalue = false;

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

	public Boolean getConcatValues() {
		return concatValues;
	}

	public void setConcatValues(Boolean concatValues) {
		this.concatValues = concatValues;
	}

	/** Can this field contain multiple values.
	 */
	public boolean isMultivalue() {
		return multivalue;
	}

	/* For result values derived from multiple input values, this is the string that will be used to separate them. */
	public String getSeparator() {
		return separator;
	}

	public void setSeparator(String separator) {
		this.separator = separator;
	}

	/**
	 * Controls whether the search index field this instance of SolrField is generating is defined
	 * as accepting multiple values (a collection of values).
	 *
	 * @param multivalue
	 */
	public void setMultivalue(boolean multivalue) {
		this.multivalue = multivalue;
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
