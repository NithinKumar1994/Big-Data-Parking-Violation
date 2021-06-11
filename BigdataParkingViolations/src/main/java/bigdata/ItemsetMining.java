package bigdata;

import java.sql.*;
/**
 * Program to generate Frequent item sets of Precinct and Violation Codes
 */
public class ItemsetMining {
    public static void main(String[] args) {
        Connection con= null;
        PreparedStatement st=null;
        ResultSet rs=null;

        String url="jdbc:postgresql://localhost:5433/postgres";
        String username="postgres";
        String password="Nithin_4567";

        long start=System.currentTimeMillis();
        try {

            try {
                con = DriverManager.getConnection(url, username, password);
                con.setAutoCommit(true);
                System.out.println("Connected to the PostgreSQL server successfully.");
                //creating a table with combinations of precinct and violation code
                con.prepareStatement("CREATE Table ItemSet as\n" +
                        "SELECT DISTINCT precinct, violationcode FROM public.\"ParkingViolation\" LIMIT 1000000;\n").execute();

                //Creating V1 from itemset table
                con.prepareStatement("Create Table V1 as\n" +
                        "select a.precinct as precinct1,count(*) as count from public.itemset a group by a.precinct having count(*)>=75; \n").execute();
                rs=con.prepareStatement("SELECT COUNT(*) FROM V1").executeQuery();
                rs.next();
                System.out.println("Created V1 table");
                System.out.println("No. of rows "+Integer.parseInt(rs.getString(1)) +"\n");
                int count=1,k=0;
                //Creating Lattice table using apriori Gen algorithm

                for(k=2;count!=0;k++) {
                    aprioriGen(con, k);
                    System.out.println("Created V"+k+" table");
                    rs=con.prepareStatement("SELECT COUNT(*) FROM V"+k).executeQuery();
                    rs.next();
                    count=Integer.parseInt(rs.getString(1));
                    System.out.println("No. of rows "+count +"\n");
                    if(count==0)
                        System.out.println("Table V"+k+" is empty");


                }

                System.out.println("The final non empty Lattice is V"+(k-2));


            }
            finally {
                con.close();
            }
        }
        catch(Exception e){
            System.out.println(e);
        }


    }

    /**
     * Function to create the lattices of the precinct and violation codes.
     * @param con sending the connection variable to get the data from postgres
     * @param k k is the lattice number that is supposed to be created
     * @throws SQLException
     */
    public static  void aprioriGen(Connection con,int k) throws SQLException {

        String condition="",Itemset="",columns="",precinctCondition="",violationCondition="";

        //storing the parts of the query that need to be generated in a string
        int i=1;
        while(i<k){
            Itemset += ", public.itemset p"+i;
            columns+=" p.precinct"+i+",";
            precinctCondition+="and p"+i+".precinct = p.precinct"+i+"\n";
            violationCondition+="and p"+i+".violationcode= p"+(i+1)+".violationcode \n";
            if(i+1==k){
                Itemset += ", public.itemset p"+k;
                precinctCondition+="and p"+k+".precinct = q.precinct"+i+"\n";
                columns+=" q.precinct"+i;
                if(i==1)
                    condition+="p.precinct"+i+" < q.precinct"+i+"\n";
                else
                    condition+="and p.precinct"+i+" < q.precinct"+i+"\n";
            }
            else {
                if(i==1)
                    condition += "p.precinct" + i + "= q.precinct" + i + "\n";
                else
                    condition += "and p.precinct" + i + "= q.precinct" + i + "\n";
            }
            i++;
        }
        System.out.println("CREATE TABLE V"+k+" AS \n" +
                "select"+columns+" as precinct"+k+" ,count(*) from V"+(k-1)+" p , V"+(k-1)+" q "+ Itemset+"\n" +
                "where "+condition+ precinctCondition+violationCondition+
                "group by "+columns+"\n" +
                "having count(*)>=75;");
        //creating Lattice table
        con.prepareStatement("CREATE TABLE V"+k+" AS \n" +
                "select"+columns+" as precinct"+k+" ,count(*) from V"+(k-1)+" p , V"+(k-1)+" q "+ Itemset+"\n" +
                "where "+condition+ precinctCondition+violationCondition+
                "group by "+columns+"\n" +
                "having count(*)>=75;").execute();

    }
}

