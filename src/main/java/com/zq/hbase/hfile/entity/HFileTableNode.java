package com.zq.hbase.hfile.entity;

public class HFileTableNode {
    String name;
    String nameAsString;
    String namespace;
    String namespaceAsString;
    String qualifier;
    String qualifierAsString;
    String systemTable;
    String nameWithNamespaceInclAsString;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getNameAsString() {
        return nameAsString;
    }

    public void setNameAsString(String nameAsString) {
        this.nameAsString = nameAsString;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getNamespaceAsString() {
        return namespaceAsString;
    }

    public void setNamespaceAsString(String namespaceAsString) {
        this.namespaceAsString = namespaceAsString;
    }

    public String getQualifier() {
        return qualifier;
    }

    public void setQualifier(String qualifier) {
        this.qualifier = qualifier;
    }

    public String getQualifierAsString() {
        return qualifierAsString;
    }

    public void setQualifierAsString(String qualifierAsString) {
        this.qualifierAsString = qualifierAsString;
    }

    public String getSystemTable() {
        return systemTable;
    }

    public void setSystemTable(String systemTable) {
        this.systemTable = systemTable;
    }

    public String getNameWithNamespaceInclAsString() {
        return nameWithNamespaceInclAsString;
    }

    public void setNameWithNamespaceInclAsString(String nameWithNamespaceInclAsString) {
        this.nameWithNamespaceInclAsString = nameWithNamespaceInclAsString;
    }

    @Override
    public String toString() {
        return "HFileTableNode{" +
                "name='" + name + '\'' +
                ", nameAsString='" + nameAsString + '\'' +
                ", namespace='" + namespace + '\'' +
                ", namespaceAsString='" + namespaceAsString + '\'' +
                ", qualifier='" + qualifier + '\'' +
                ", qualifierAsString='" + qualifierAsString + '\'' +
                ", systemTable='" + systemTable + '\'' +
                ", nameWithNamespaceInclAsString='" + nameWithNamespaceInclAsString + '\'' +
                '}';
    }
}
