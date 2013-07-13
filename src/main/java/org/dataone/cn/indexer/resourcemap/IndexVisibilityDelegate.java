package org.dataone.cn.indexer.resourcemap;

import org.dataone.service.types.v1.Identifier;

public interface IndexVisibilityDelegate {

    public boolean isDocumentVisible(Identifier pid);

}
