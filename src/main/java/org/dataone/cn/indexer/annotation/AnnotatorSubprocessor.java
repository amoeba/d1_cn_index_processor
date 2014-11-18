package org.dataone.cn.indexer.annotation;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.cn.indexer.parser.IDocumentSubprocessor;
import org.dataone.cn.indexer.solrhttp.SolrDoc;
import org.dataone.cn.indexer.solrhttp.SolrElementField;
import org.dataone.configuration.Settings;
import org.w3c.dom.Document;


/**
 * The intent of this subprocessor is to fetch annotations about the given 
 * documents as housed in an external store and add them to the solr index.
 * As of November 2014, this is still experimental for the semantic goals of 
 * the project.
 * @author leinfelder
 *
 */
public class AnnotatorSubprocessor implements IDocumentSubprocessor {

	private static Log log = LogFactory.getLog(AnnotatorSubprocessor.class);

	@Override
	public boolean canProcess(Document doc) throws XPathExpressionException {
		// process any document, looking for annotations about said document
		return true;
	}

	@Override
	public void initExpression(XPath xpath) {
		// can decide later to limit type of documents that support annotation lookup
		
	}

	@Override
	public Map<String, SolrDoc> processDocument(String identifier,
			Map<String, SolrDoc> docs, Document doc) throws Exception {
		
		// for each document, check if there are any annotations
		Iterator<Entry<String, SolrDoc>> entries = docs.entrySet().iterator();
		while (entries.hasNext()) {
			Entry<String, SolrDoc> entry = entries.next();
			String pid = entry.getKey();
			SolrDoc solrDoc = entry.getValue();
			Map<String, List<Object>> annotations = lookUpAnnotations(pid);
			// annotations found, then process them
			if (annotations != null) {
				Iterator<Entry<String, List<Object>>> fieldEntries = annotations.entrySet().iterator();
				// each field can have multiple values
				while (fieldEntries.hasNext()) {
					Entry<String, List<Object>> fieldEntry = fieldEntries.next();
					String name = fieldEntry.getKey();
					// add each value as a solr field
					for (Object value: fieldEntry.getValue()) {
						SolrElementField solrField = new SolrElementField(name, value.toString());
						solrDoc.addField(solrField);
					}
				}
			}
		}
		
		// return the collection that we have augmented
		return docs;
	}
	
    /**
	 * Look up annotations from annotator service
	 * @see "http://docs.annotatorjs.org/en/latest/storage.html"
	 * @param pid the identifier to fetch annotations about
	 * @return
	 */
	public static Map<String, List<Object>> lookUpAnnotations(String pid) {
		

		String annotatorUrl = null;
		String consumerKey = null;
		try {
			
			annotatorUrl = Settings.getConfiguration().getString("annotator.store.url");
			consumerKey = Settings.getConfiguration().getString("annotator.consumerKey");

			// skip if not configured to query the annotator-store
			if (annotatorUrl == null || annotatorUrl.length() == 0) {
				return null;
			}
			
			// TODO: query for matching PID only - wasting time iterating over full list
			//String urlParameters = "pid=" + URLEncoder.encode(pid, "UTF-8");
			String urlParameters = "consumer=" + consumerKey;
			
			String url = annotatorUrl + "?" + urlParameters;
			HttpClient client = new HttpClient();
			HttpMethod method = new GetMethod(url);
			method.addRequestHeader("Accept", "application/json");
			client.executeMethod(method);
			InputStream is = method.getResponseBodyAsStream();
			
			String results = IOUtils.toString(is, "UTF-8");
			log.debug("RESULTS: " + results);
			JSONObject jo = (JSONObject) JSONValue.parse(results);
			
			JSONArray rows = (JSONArray) jo.get("rows");
			int count = rows.size();
			Map<String, List<Object>> annotations = new HashMap<String, List<Object>>();
			
			// use catch-all annotation field for the tags
			List<Object> tagValues = null;
			String tagKey = "annotation_sm";
			
			// track the comments here
			List<Object> commentValues = null;
			String commentKey = "comment_sm";
			
			for (int i = 0; i < count; i++){
				JSONObject row = (JSONObject) rows.get(i);
				
				// skip this row if it is not about this pid
				// FIXME: Bug in annotator-store prevents effective search by pid
				String pidValue = row.get("pid").toString();
				if (!pidValue.equals(pid)) {
					continue;
				}
				
				// index the (semantic) tags
				// if the annotation told us the target index field, then use it
				Object field = row.get("field");
				if (field != null) {
					tagKey = field.toString();
				}
				
				// make sure we have a place to store the values
				tagValues = annotations.get(tagKey);
				if (tagValues == null) {
					tagValues = new ArrayList<Object>();
				}
				Object obj = row.get("tags");
				if (obj instanceof JSONArray) {
					JSONArray tags = (JSONArray) obj;
					tagValues.addAll(tags);
				} else {
					String value = obj.toString();
					tagValues.add(value);
				}
				annotations.put(tagKey, tagValues);
				
				// index the comments
				commentValues = annotations.get(commentKey);
				if (commentValues == null) {
					commentValues = new ArrayList<Object>();
				}
				Object commentObj = row.get("text");
				if (commentObj != null) {
					String value = commentObj.toString();
					if (value != null && value.length() > 0) {
						commentValues.add(value);
					}
				}
				annotations.put(commentKey, commentValues);

			}
			// just populate this one field for example
			return annotations;
			
		} catch (Exception e) {
			log.error("Could not lookup annotation using: " + annotatorUrl, e);
		}
		
		return null;
	}

}
