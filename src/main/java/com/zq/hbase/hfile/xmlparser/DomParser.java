package com.zq.hbase.hfile.xmlparser;




import com.zq.hbase.hfile.entity.FsimageInode;
import com.zq.hbase.hfile.entity.TableNode;
import org.apache.commons.lang.ArrayUtils;
import org.dom4j.Element;
import org.dom4j.io.DOMReader;



import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class DomParser {
    public static void main(String[] args) throws ParserConfigurationException, IOException, SAXException {
        String filePath="D:\\HbaseData\\Hbasedata\\fsimage.xml";
        HashMap<String, FsimageInode> nodeMap = getStringFsimageInodeHashMap(filePath);

        //找到根节点
        FsimageInode node=findRoot(nodeMap);
        HashMap<String,TableNode> tableNodeHashMap=new HashMap<String,TableNode>();
        //生成biao和block的映射
        generateTableNode(node,tableNodeHashMap);
        System.out.println("root"+ node.toString());
    }

    private static HashMap<String, FsimageInode> getStringFsimageInodeHashMap(String filePath) throws ParserConfigurationException, SAXException, IOException {
        HashMap<String,FsimageInode> nodeMap =new HashMap<String, FsimageInode>();
        //initiate
        File file=new File(filePath);
        Element root = Dom4jInitiate(file);
        //生成fsimage节点
        GenerateFsimageInode(nodeMap, root);
        //生成关系
        GenerateChildList(nodeMap, root);
        Iterator iter=nodeMap.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry) iter.next();
            Object key = entry.getKey();
            FsimageInode val = (FsimageInode)entry.getValue();
            System.out.println(val.toString());
        }
        return nodeMap;
    }

    /**
     * 建立表和block的映射
     * @param node
     * @param tableNodeHashMap
     */
    private static void generateTableNode(FsimageInode node, HashMap<String, TableNode> tableNodeHashMap) {
               FsimageInode nameSpaceNode=node.getChildMap().get("hbase").getChildMap().get("data").getChildMap().get("default");
                Map<String,FsimageInode>  tableNode=  nameSpaceNode.getChildMap();
               Iterator iter= tableNode.entrySet().iterator();
               TableNode table;
                while (iter.hasNext()) {
                    Map.Entry entry = (Map.Entry) iter.next();
                    Object key = entry.getKey();
                    FsimageInode val = (FsimageInode)entry.getValue();
                    table=new TableNode(val.getName(),nameSpaceNode.getName());
                    //链接表的以及block
                    LinkBlocks(table,val);
                    System.out.println("table "+ key +" "+table.toString());
                    tableNodeHashMap.put(table.getTableName(),table);
                }
          System.out.println(tableNodeHashMap);
          String tableName="hbase_student";
         return;
    }

    /**
     * 执行shell
     */

    public static void execShell(String scriptPath, String ... para) {
        try {
            String[] cmd = new String[]{scriptPath};
            //为了解决参数中包含空格
            cmd= (String[]) ArrayUtils.addAll(cmd,para);

            //解决脚本没有执行权限
            ProcessBuilder builder = new ProcessBuilder("/bin/chmod", "755",scriptPath);
            Process process = builder.start();
            process.waitFor();

            Process ps = Runtime.getRuntime().exec(cmd);
            ps.waitFor();

            BufferedReader br = new BufferedReader(new InputStreamReader(ps.getInputStream()));
            StringBuffer sb = new StringBuffer();
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line).append("\n");
            }
            //执行结果
            String result = sb.toString();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 将表和blocks链接好
     *
     * @param table
     * @param parentNode
     */
    private static void LinkBlocks(TableNode table, FsimageInode parentNode) {
        Iterator iter= parentNode.getChildMap().entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry) iter.next();
            String key =(String) entry.getKey();
            FsimageInode val = (FsimageInode)entry.getValue();
            if(key.contains("."))
            {
                continue;
            }else if(val.getType().equals("DIRECTORY"))
            {
                    LinkBlocks(table,val);
            }else if(val.getType().equals("FILE")&&val.getBlocksidList().size()!=0)
            {
              for(String block:val.getBlocksidList())
              {
                  table.addblock(block);
              }
            }

        }

    }

    /**
     * 找到根节点
     * @param nodeMap
     * @return
     */
    private static FsimageInode findRoot(HashMap<String, FsimageInode> nodeMap) {
        Iterator iter=nodeMap.entrySet().iterator();
        FsimageInode val=null;
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry) iter.next();
         String key =(String) entry.getKey();
             val = (FsimageInode)entry.getValue();
          if(val.getParentid()==null)
          {
              return val;
          }
        }
        return val;

    }

    /**
     * initiate
     * @param file
     * @return
     * @throws ParserConfigurationException
     * @throws SAXException
     * @throws IOException
     */
    private static Element Dom4jInitiate(File file) throws ParserConfigurationException, SAXException, IOException {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        org.w3c.dom.Document domDocument = db.parse(file);
        DOMReader reader = new DOMReader();
        org.dom4j.Document document = reader.read(domDocument);
        return document.getRootElement();
    }

    private static void GenerateFsimageInode(HashMap<String, FsimageInode> nodeMap, Element root) {
        Element INodesection= (Element) root.element("INodeSection");
        List inodeList=INodesection.elements("inode");
        System.out.println(inodeList.size());
        for(int i=0;i<inodeList.size();i++)
        {
         FsimageInode inode=  generateFsimageInode(( Element)inodeList.get(i));
         nodeMap.put(inode.getId(),inode);
        }
    }

    private static void GenerateChildList(HashMap<String, FsimageInode> nodeMap, Element root) {
        Element INodeDirectorySection=(Element) root.element("INodeDirectorySection");
        List DirectorySection=INodeDirectorySection.elements("directory");
        for(int j=0;j<DirectorySection.size();j++)
        {

            Element elementdir= (Element)(DirectorySection.get(j));
            String      parentId=elementdir.element("parent").getText();
            List list=elementdir.elements("inode");
            FsimageInode   node=nodeMap.get(parentId);
            FsimageInode childNode;
            for(int k=0;k<list.size();k++)
            {
               // System.out.println(((Element) list.get(k)).getText());
               childNode= nodeMap.get(((Element) list.get(k)).getText());
               childNode.setParentid(node.getId());
                node.addChild(childNode);
            }

        }
    }

    private static FsimageInode generateFsimageInode(org.dom4j.Element element) {
        FsimageInode Inode=new FsimageInode();
        Inode.setId(element.element("id").getText());
        Inode.setType(element.element("type").getText());
        Inode.setName(element.element("name").getText());
        if(element.element("blocks")!=null) {
            org.dom4j.Element blocksElement = element.element("blocks");
            List blockList = blocksElement.elements("block");
            for (int i = 0; i < blockList.size(); i++) {
                String id = ((org.dom4j.Element) blockList.get(i)).element("id").getText();
                Inode.addBlocks(id);
            }

        }
      //  System.out.println(Inode.toString());
        return Inode;
        }




    }



