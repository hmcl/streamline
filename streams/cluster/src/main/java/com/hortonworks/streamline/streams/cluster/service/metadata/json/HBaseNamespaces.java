package com.hortonworks.streamline.streams.cluster.service.metadata.json;

import org.apache.hadoop.hbase.NamespaceDescriptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Wrapper used to show proper JSON formatting
 */
public class HBaseNamespaces {
    private List<String> namespaces;

    public HBaseNamespaces(List<String> namespaces) {
        this.namespaces = namespaces;
    }

    public static HBaseNamespaces newInstance(NamespaceDescriptor[] namespaceDescriptors) {
        List<String> namespaces = Collections.emptyList();
        if (namespaceDescriptors != null) {
//            namespaces = Arrays.stream(namespaceDescriptors).map(NamespaceDescriptor::getName).collect(Collectors.toList());
            namespaces = new ArrayList<>(namespaceDescriptors.length);
            for (NamespaceDescriptor namespace : namespaceDescriptors) {
                namespaces.add(namespace.getName());
            }
        }
        return new HBaseNamespaces(namespaces);
    }

    public List<String> getNamespaces() {
        return namespaces;
    }
}
