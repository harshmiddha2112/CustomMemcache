/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package custommemcache;
import java.util.*;

/**
 *
 * @author harshmiddha
 */
public class Main {

    /**
     * @param args the command line arguments
     */
    
    
    public static void main(String[] args) {

    Scanner sc=new Scanner(System.in);

    CustomMemcache cc = new CustomMemcache();
    System.out.println("Hi Welcome\n List of operations supported are \n set key value,\n"
                + " get key,\n delete key,\n count value,\n start,\n commit,\n rollback\n"
            + " Note:Key can't start with count..its reserved keyword\n\n");

    while(true) {

        String operationStr = sc.next();
        operationStr += sc.nextLine();

        cc.processInputString(operationStr);

        if(operationStr.equals("break")){
            break;
        }
    }  

    sc.close();  
    }
    
    
    
}
