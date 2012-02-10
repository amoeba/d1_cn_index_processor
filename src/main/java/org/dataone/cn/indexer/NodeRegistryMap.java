package org.dataone.cn.indexer;

import java.util.HashMap;

import org.dataone.service.types.v1.Node;

public class NodeRegistryMap extends HashMap<String, Node> {
  
  public String getName(String nodeid) {
    String name = this.get(nodeid).getName();
    return name;
  }

}
