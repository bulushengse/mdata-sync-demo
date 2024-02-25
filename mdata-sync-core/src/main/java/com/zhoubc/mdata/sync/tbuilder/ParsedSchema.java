package com.zhoubc.mdata.sync.tbuilder;

import com.google.common.collect.Sets;
import com.zhoubc.mdata.sync.tbuilder.model.Relationship;
import com.zhoubc.mdata.sync.tconfig.ModelDefination;
import com.zhoubc.mdata.sync.tbuilder.model.NodeInfo;
import com.zhoubc.mdata.sync.tconfig.OutputConfig;
import com.zhoubc.mdata.sync.utils.MapUtil;
import com.zhoubc.mdata.sync.utils.StringPair;
import com.zhoubc.mdata.sync.utils.Validate;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.lang.invoke.MethodHandle;
import java.util.*;

/**
 * @author zhoubc
 * @description: TODO
 * @date 2023/9/24 17:16
 */
public class ParsedSchema {
    private static final Logger logger = LoggerFactory.getLogger(IndexBuilder.class);


    // ES索引表名称
    private String scriptKey;

    // ES表结构配置文件
    private OutputConfig outputConfig;

    //   DB表 ：表主键字段名
    private Map<String, NodeInfo> relatedTables;

    private String primaryDatabase;
    private String primaryTableName;
    private String primaryDatabaseAndTable;
    private ModelDefination primaryModelDefination;

    //private Pair<String, List<>> primarySelectSql;

    // 关联语句  db.table
    private Map<StringPair, Relationship> relatedRelationMap;


    private MethodHandle tableDataBacktrackingHandler;


    // groovy
    private String generatedCodes;


    // 忽略的表    "db.table"
    private Set<String> ignoredTables;




    public ParsedSchema(OutputConfig cf){

        Validate.assertNotNull(cf, "");
        Validate.assertNotEmpty(cf.getDestination(), "");
        Validate.assertNotEmpty(cf.getTableDataBacktrackingHandler(), "");
        Validate.assertNotEmptyList(cf.getModelList(), "");

        this.outputConfig = cf;
        this.scriptKey = cf.getDestination();
        logger.info("开始构建ParsedSchema:{}", this.scriptKey);

        //
        if (StringUtils.isNotEmpty(cf.getIgnoredTablesWhenExecution())) {
            String[] ignoredTableArray = StringUtils.split(cf.getIgnoredTablesWhenExecution(),",");
            this.ignoredTables = Sets.newHashSet(ignoredTableArray);
        }

        //
        Set<String> relatedDBTableSet = MapUtil.set();
        for (ModelDefination table : cf.getModelList()) {

            //校验数据库存不存在 缺少

            String databaseAndTable = table.getDataSourceTableName();

            relatedDBTableSet.add(databaseAndTable);

            int tableFlag = 0;
            if (this.ignoredTables !=null && this.ignoredTables.contains(databaseAndTable)) {
                tableFlag = -1;
            }
            this.relatedTables.put(databaseAndTable, NodeInfo.of(tableFlag,databaseAndTable,null,table.getPrimaryKey()));

        }


        //
        List<Relationship> rlist = null;
        if (!CollectionUtils.isEmpty(cf.getRelationList())) {
            rlist = new ArrayList(cf.getRelationList().size());
            for (String relationStr : cf.getRelationList()) {
                if (StringUtils.isNotEmpty(relationStr)) {
                    rlist.add(Relationship.createRelationship(relationStr));
                }
            }
        }

        if (!CollectionUtils.isEmpty(rlist)) {


            for (Relationship relationShip : rlist) {

                String tableAndField = relationShip.getDataSource() + "." + relationShip.getEntityCode();
                String refTableAndField = relationShip.getReferencedDataSource() + "." + relationShip.getReferencedEntityCode();

                boolean existsChecker = relatedDBTableSet.contains(tableAndField);
                boolean existsRChecker = relatedDBTableSet.contains(refTableAndField);
                if (existsChecker && existsRChecker) {




                    StringPair sp = StringPair.of(tableAndField, refTableAndField);
                    this.relatedRelationMap.put(sp, relationShip);







                }


            }



        }














    }


//    public Map executePrimary(){
//
//    }






}
