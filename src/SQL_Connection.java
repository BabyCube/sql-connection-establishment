import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

//use NANO time to accurately record execution time
import java.util.concurrent.TimeUnit;

public class SQL_Connection {
	
    /* Database credentials */
    String user = "cse4701";
    String password = "intersect";
    String host = "query.engr.uconn.edu";
    String port = "1521";
    String sid = "BIBCI";
    String url = "jdbc:oracle:thin:@" + host + ":" + port + ":" + sid;
    
    //keep track of the time for each execution
    long startTime;
    long endTime;
    float duration;

	
	Connection con = null;
	Statement stmt;
    ResultSet rs;
    ResultSetMetaData rsmd;

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		//change to change the comparison data base!
		int input = 2;
		
		
		if (input == 1)
		{
			SQL_Connection C1 = new SQL_Connection();
			C1.comparison1();
		}else if (input == 2) {
			SQL_Connection C2 = new SQL_Connection();
			C2.comparison2();
		}


		
    }
	
	void comparison1() {
		
		int queryID = 0;

		
		try {
			
			DriverManager.registerDriver(new oracle.jdbc.OracleDriver()); 
			
			// the connection keychain has been modified to make the file connect to desired database
			// UCONN IP required for connection
			con = DriverManager.getConnection(url, user, password);
			
            stmt = con.createStatement();

            //test the connection and output! compare it with the data output from qurey directly made in Oracle SQL developer!
            //String sql = "select SETID from G1 where geneid = 1";
            
            //SQL string should be written in SQL statement!
           
            for (queryID = 0; queryID < 51; queryID++){
            	//start counting time for each distinct Q_i
            	startTime = System.nanoTime();
            	
            //first SQL: extracts all the geneIDs for this queryID
            String sql_intermediate = "select SETID as querySETID, query_count, GENEID as geneExtracted from "
            		+ "((select SETID as id, count(*) as query_count from QUERY group by SETID)"
            		+ " join QUERY on id = SETID) where SETID = " + queryID;
            String sql_intermediate2 = "select SETID, GENEID, set_count from ((select SETID as id, count(*) as set_count from G1 group by SETID) join G1 on id = SETID)";
            String sql_intermediate3 = "select querySETID, query_count, SETID, set_count from ((" + sql_intermediate + ")join(" + sql_intermediate2 + " )on geneExtracted = geneID ) "
            		+ "order by SETID";
            String sql_intermediate4 = "select querySETID, query_count, SETID, set_count, COUNT(*) as overlap_count from (" + sql_intermediate3 
            		+ ") group by querySETID, query_count, SETID, set_count order by overlap_count desc";
            String sql_intermediate5 = "select * from (" + sql_intermediate4 + ") where rownum < 6  ";
            String sql = sql_intermediate5;

            rs = stmt.executeQuery(sql);
            rsmd = rs.getMetaData();

            
        	//end counting time for each distinct Q_i
        	endTime = System.nanoTime();
        	duration = (float) (endTime - startTime);
        	

            //the connection test program provided 
            while (rs.next()) {
            	
                ArrayList<Object> obArray = new ArrayList<Object>();
                for (int i = 0; i < rsmd.getColumnCount(); i++) {
                    obArray.add(rs.getObject(i + 1));
                   // if (i = 0) {}
                    System.out.print(obArray.toArray()[i] + "\t");
                }
                System.out.println(duration);
            }
            
            }
            
        	//close the connection when done
        		con.close();

        		
        } catch (SQLException ex) {
            Logger.getLogger(SQL_Connection.class.getName()).log(Level.SEVERE, null, ex);
        }
		
		
		
	}
	
	void comparison2() {
		
		int queryID = 0;
		
		try {
			
			DriverManager.registerDriver(new oracle.jdbc.OracleDriver()); 
			
			// the connection keychain has been modified to make the file connect to desired database
			// UCONN IP required for connection
			con = DriverManager.getConnection(url, user, password);
			
            stmt = con.createStatement();

            //test the connection and output! compare it with the data output from qurey directly made in Oracle SQL developer!
            //String sql = "select SETID from G1 where geneid = 1";
            
            //SQL string should be written in SQL statement!
           
            for (queryID = 0; queryID < 51; queryID++){
            	//start counting time for each distinct Q_i
            	startTime = System.nanoTime();
            	
            //first SQL: extracts all the geneIDs for this queryID
            String sql_intermediate = "select SETID as querySETID, query_count, GENEID as geneExtracted from "
            		+ "((select SETID as id, count(*) as query_count from QUERY group by SETID)"
            		+ " join QUERY on id = SETID) where SETID = " + queryID;
            String sql_intermediate2 = "select SETID, GENEID, set_count from ((select SETID as id, count(*) as set_count from G2 group by SETID) join G2 on id = SETID)";
            String sql_intermediate3 = "select querySETID, query_count, SETID, set_count from ((" + sql_intermediate + ")join(" + sql_intermediate2 + " )on geneExtracted = geneID ) "
            		+ "order by SETID";
            String sql_intermediate4 = "select querySETID, query_count, SETID, set_count, COUNT(*) as overlap_count from (" + sql_intermediate3 
            		+ ") group by querySETID, query_count, SETID, set_count order by overlap_count desc";
            String sql_intermediate5 = "select * from (" + sql_intermediate4 + ") where rownum < 6  ";
            String sql = sql_intermediate5;

            rs = stmt.executeQuery(sql);
            rsmd = rs.getMetaData();

            
        	//end counting time for each distinct Q_i
        	endTime = System.nanoTime();
        	duration = (float) (endTime - startTime);
        	

            //the connection test program provided 
            while (rs.next()) {
            	
                ArrayList<Object> obArray = new ArrayList<Object>();
                for (int i = 0; i < rsmd.getColumnCount(); i++) {
                    obArray.add(rs.getObject(i + 1));
                   // if (i = 0) {}
                    System.out.print(obArray.toArray()[i] + "\t");
                }
                System.out.println(duration);
            }
            
            }
            
        	//close the connection when done
        		con.close();

        		
        } catch (SQLException ex) {
            Logger.getLogger(SQL_Connection.class.getName()).log(Level.SEVERE, null, ex);
        }
		
		
		
	}


}
