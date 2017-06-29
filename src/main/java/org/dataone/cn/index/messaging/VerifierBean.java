package org.dataone.cn.index.messaging;

import org.dataone.cn.indexer.D1IndexerSolrClient;
import org.dataone.cn.indexer.SolrIndexService;
import org.dataone.configuration.Settings;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.ApplicationContextException;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.PriorityOrdered;
import org.springframework.core.env.Environment;

@Configuration
public class VerifierBean implements BeanFactoryPostProcessor, PriorityOrdered {

    public VerifierBean() {
        // TODO Auto-generated constructor stub
    }

    @Override
    public void postProcessBeanFactory(final ConfigurableListableBeanFactory beanFactory)
            throws BeansException {
        
        System.out.println("....... Entering VerifierBean for Verifying Spring startup ........");
        
        Environment env = (Environment) beanFactory.getBean(Environment.class);
        
        
        Settings.getConfiguration();
        
        SolrIndexService sis = (SolrIndexService) beanFactory.getBean(SolrIndexService.class);
        if (sis == null) {
            throw new ApplicationContextException("Missing SolrIndexService bean at startup");
        }
        
        String solrQueryUri = (String) beanFactory.getBean("solrQueryUri");
        if (solrQueryUri == null) { 
            throw new ApplicationContextException("Missing solrQueryUri bean at startup");
        } else {
            System.out.println(solrQueryUri);
        }   
        
        D1IndexerSolrClient client = (D1IndexerSolrClient) beanFactory.getBean(D1IndexerSolrClient.class);
        if (client == null) {
            throw new ApplicationContextException("Missing D1IndexerSolrClient bean at startup");
        } else {
            System.out.println("D1IndexerSolrClient: " + client);
        }
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }

}
