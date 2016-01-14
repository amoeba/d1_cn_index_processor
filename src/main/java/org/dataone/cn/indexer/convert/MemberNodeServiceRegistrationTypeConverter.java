package org.dataone.cn.indexer.convert;

import java.util.Collection;

import org.dataone.cn.indexer.parser.utility.MemberNodeServiceRegistrationTypeCache;
import org.springframework.beans.factory.annotation.Autowired;

public class MemberNodeServiceRegistrationTypeConverter implements IConverter {

    @Autowired
    private MemberNodeServiceRegistrationTypeCache mnServiceRegistrationTypeCacheService;

    @Override
    public String convert(String data) {

        Collection<MemberNodeServiceRegistrationType> serviceTypes = mnServiceRegistrationTypeCacheService
                .getServiceTypes();

        for (MemberNodeServiceRegistrationType serviceType : serviceTypes)
            for (String matchPattern : serviceType.getMatchingPatterns())
                if (data.matches(matchPattern))
                    return serviceType.getName();

        return data;
    }

}
