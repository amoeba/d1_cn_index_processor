package org.dataone.cn.indexer.resourcemap;

import org.dataone.service.types.v1.Identifier;

public class IndexVisibilityDelegateTestImpl implements IndexVisibilityDelegate {

    public boolean isDocumentVisible(Identifier pid) {
        return true;
    }

    public boolean documentExists(Identifier pid) {
        return true;
    }

}
