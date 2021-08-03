package org.dataone.cn.indexer.parser;

import com.hp.hpl.jena.query.Dataset;
import org.dataone.cn.indexer.parser.utility.ElementCombiner;
import org.dataone.cn.indexer.solrhttp.SolrElementField;

import java.util.List;

public interface ISolrDatasetField extends ISolrDataField {

    public ElementCombiner getBase();

    public void setBase(ElementCombiner base);

    //public List<SolrElementField> getDerivedFields(Dataset dataset);
    public List<SolrElementField> getFields(Dataset dataset);

}
