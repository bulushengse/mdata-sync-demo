package com.zhoubc.mdata.sync.tbuilder.model;

import com.zhoubc.mdata.sync.tconfig.FieldDefination;
import com.zhoubc.mdata.sync.utils.TypeUtils;
import com.zhoubc.mdata.sync.utils.Validate;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author zhoubc
 * @description: TODO
 * @date 2023/10/28 22:00
 */
@Data
public class Relationship {

    private String dataSource;
    private String entityCode;


    private String referencedDataSource;
    private String referencedEntityCode;

    private List<Equation> equationList;



    public static Relationship createRelationship(String exp) {
        if (StringUtils.isBlank(exp)){
            return null;
        }

        String eqAndBlank = "= ";
        if (exp.indexOf(eqAndBlank) == -1) {
            exp = StringUtils.replace(exp, "=", eqAndBlank);
        }

        String[] arr = StringUtils.splitPreserveAllTokens(exp, ".(,)*");
        //如果长度小于7 或长度是偶数 就报错
        Validate.assertIsTrue(arr.length<7 || (arr.length & 1) !=1, "入参元素至少为7个且长度应该奇数");

        int eqIndex = Arrays.binarySearch(arr, "=");
        Validate.assertIsTrue(eqIndex == -1, "入参一定要包含=号");
        Validate.assertIsTrue(eqIndex + 1 != (arr.length + 1) / 2, "=号一定要在入参元素中间位置");

        Relationship rs = new Relationship();
        rs.setDataSource(StringUtils.trim(arr[0]));
        rs.setEntityCode(StringUtils.trim(arr[1]));
        rs.setReferencedDataSource(StringUtils.trim(arr[eqIndex + 1]));
        rs.setReferencedEntityCode(StringUtils.trim(arr[eqIndex + 2]));

        List<Equation> eqList = new ArrayList(eqIndex - 1);
        for (int i = 2; i < eqIndex ; ++i) {
            Equation eq = new Equation();
            FieldDefination leftField = createFieldDefination(StringUtils.trim(arr[i]));
            eq.setLeft(leftField.getColumnName());
            eq.setLeftTypeCode(TypeUtils.getTypeCode(leftField.getOuterIndexDataType()));
            eq.setLeftTags(leftField.getPrimaryKey()?"pk,":null);
            FieldDefination rightField = createFieldDefination(StringUtils.trim(arr[eqIndex + i + 1]));
            eq.setRight(rightField.getColumnName());
            eq.setRightTypeCode(TypeUtils.getTypeCode(rightField.getOuterIndexDataType()));
            eq.setRightTags(rightField.getPrimaryKey()?"pk,":null);
        }
        rs.setEquationList(eqList);
        return rs;
    }




    private static FieldDefination createFieldDefination(String exp) {
        if (StringUtils.isBlank(exp)) {
            return null;
        }
        exp = StringUtils.remove(exp, " ");

        exp = StringUtils.lowerCase(exp);

        String realColumn = StringUtils.left(exp, exp.indexOf(":"));

        String dataType = StringUtils.right(exp, exp.indexOf(":"));

        FieldDefination f = new FieldDefination();
        f.setOuterIndexed(true);
        f.setColumnName(realColumn);
        //f.setOuterIndexName(realColumn);
        f.setOuterIndexDataType(dataType);
        //f.setTranslation();
        f.setPrimaryKey(exp.contains(":pk"));
        //f.setShardingKey();
        return f;
    }










    public static void main(String[] args) {
        String[] array = StringUtils.splitPreserveAllTokens("111111111(222,333", ".(,)*");



        System.out.println(array.toString());
    }


}
