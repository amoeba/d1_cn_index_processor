package org.dataone.cn.indexer.annotation;

import static org.junit.Assert.*;

import java.util.Map;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "../../index/test-context.xml" })

public class OntologyModelServiceTest {
	@Test
	public void testConceptNotFoundExpansion() {
		/**
		 * Test that the OntologyModelService returns just the concept you asked it to expand when
		 * we expect that no pre-loaded ontologies define superclasses for it.
		 */
		try {
			Map<String, Set<String>> concepts = OntologyModelService.getInstance().expandConcepts("https://example.org");

			// Assert on Solr fields returend
			Set<String> fields = new HashSet<String>();
			fields.add("annotation_property_uri");
			fields.add("annotation_value_uri");
			assertEquals(concepts.keySet(), fields);

			// Assert on Solr field values returned
			Set<String> values = new HashSet<String>();
			values.add("https://example.org");
			assertEquals(values, concepts.get("annotation_property_uri"));
			assertEquals(values, concepts.get("annotation_value_uri"));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testECSOMeasurementTypeExpansion() {
		/**
		 *
		 */
		try {
			Map<String, Set<String>> concepts = OntologyModelService.getInstance().expandConcepts("http://purl.dataone.org/odo/ECSO_00000543");

			// Assert on Solr fields returend
			Set<String> fields = new HashSet<String>();
			fields.add("annotation_property_uri");
			fields.add("annotation_value_uri");
			assertEquals(concepts.keySet(), fields);

			// Assert on Solr field values returned
			Set<String> values = new HashSet<String>();
			values.add("http://purl.dataone.org/odo/ECSO_00000536");
			values.add("http://purl.dataone.org/odo/ECSO_00000543");
			values.add("http://purl.dataone.org/odo/ECSO_00001105");
			values.add("http://purl.dataone.org/odo/ECSO_00000514");
			values.add("http://ecoinformatics.org/oboe/oboe.1.2/oboe-core.owl#MeasurementType");
			values.add("http://www.w3.org/2000/01/rdf-schema#Resource");

			assertEquals(values, concepts.get("annotation_value_uri"));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testPropertyExpansion() {
		try {
			Map<String, Set<String>> concepts = OntologyModelService.getInstance().expandConcepts("http://purl.obolibrary.org/obo/RO_0002352");

			Set<String> values = new HashSet<String>();
			values.add("http://purl.obolibrary.org/obo/RO_0002328");
			values.add("http://purl.obolibrary.org/obo/RO_0002352");
			values.add("http://purl.obolibrary.org/obo/RO_0000056");

			assertEquals(values, concepts.get("annotation_property_uri"));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
