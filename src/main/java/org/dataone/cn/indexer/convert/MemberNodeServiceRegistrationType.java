package org.dataone.cn.indexer.convert;

import java.util.Collection;

public class MemberNodeServiceRegistrationType {

    private String name;
    private Collection<String> matchingPatterns;

    public MemberNodeServiceRegistrationType() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Collection<String> getMatchingPatterns() {
        return matchingPatterns;
    }

    public void setMatchingPatterns(Collection<String> matchingPatterns) {
        this.matchingPatterns = matchingPatterns;
    }

}
