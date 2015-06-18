package org.dataone.cn.indexer.annotation;

import static org.junit.Assert.*;

import java.util.Map;
import java.util.Set;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "../../index/test-context.xml", "test-context-annotator.xml" })

public class AnnotatorSubprocessorTest {
	
	@Autowired
	private AnnotatorSubprocessor annotatorSubprocessor;
	
	//@Autowired
	private String annotationUri = "http://ecoinformatics.org/oboe-ext/sbclter.1.0/oboe-sbclter.owl#WetMass";
	
	//@Autowired
	private String expectedUri = "http://ecoinformatics.org/oboe/oboe.1.0/oboe-characteristics.owl#Mass";

	
	@Test
	public void testConceptExpansion() {
		
		try {
			Map<String, Set<String>> concepts = annotatorSubprocessor.expandConcepts(annotationUri);
			for (Set<String> conceptSet: concepts.values()) {
				assertTrue(conceptSet.contains(expectedUri));
				return;
			}
			fail("Should have returned expected concept");
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail(e.getMessage());
		}
		
	}
	
	@Test
	public void testConceptExpansionECSO() {
		
		try {
			String subclassUri = "http://purl.dataone.org/odo/ECSO_00000039";
			String superclassUri = "http://purl.dataone.org/odo/ECSO_00000010";

			Map<String, Set<String>> concepts = annotatorSubprocessor.expandConcepts(subclassUri);
			for (Set<String> conceptSet: concepts.values()) {
				assertTrue(conceptSet.contains(superclassUri));
				return;
			}
			fail("Should have returned superclass concept");
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail(e.getMessage());
		}
		
	}

}
