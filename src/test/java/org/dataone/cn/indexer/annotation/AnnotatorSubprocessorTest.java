package org.dataone.cn.indexer.annotation;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "../../index/test-context.xml", "test-context-annotator.xml" })

/**
 * This test is basically empty at the moment. Most of the functionality of the
 * AnnotatorSubprocessor is handled by the OntologyModelService but it would be
 * good to test the remaining, untested methods.
 */
public class AnnotatorSubprocessorTest {

	@Autowired
	private AnnotatorSubprocessor annotatorSubprocessor;

	@Test
	public void testCanProcess() {
		assertTrue(annotatorSubprocessor.canProcess("http://docs.annotatorjs.org/en/v1.2.x/annotation-format.html"));
	}
}
