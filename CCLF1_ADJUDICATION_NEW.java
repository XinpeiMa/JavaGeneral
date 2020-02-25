package MyCode;

import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Set;


/**
 * cclf1 adjudication
 * @XinpeiMa
 */



public class CCLF1_ADJUDICATION_NEW {
    /**
     * variables
     */
    // input fields
    private LinkedList<String> CurrentClaimUniqueIdentifier = new LinkedList<String>();
    private LinkedList<String> ProviderOscar_Number = new LinkedList<String>();
    private LinkedList<String> BeneficiaryHIC_Number = new LinkedList<String>();
    private LinkedList<String> ClaimFromDate = new LinkedList<String>();
    private LinkedList<String> ClaimThroughDate = new LinkedList<String>();
    private LinkedList<String> BeneficiaryEquitableBIC_HICN_Number = new LinkedList<String>();
    private LinkedList<String> ClaimAdjustmentTypeCode = new LinkedList<String>();
    // output fields
    private LinkedList<String> DeleteClaim = new LinkedList<String>();
    private LinkedList<String> ClaimDeleteReason = new LinkedList<String>();
    private Connection conn;
    // the number of records
    private int rRecs = 0;
    // output path
    private String outputPath = "[ACODB].[xma].[TestCCLF1]";

    /**
     * read cclf1 data from SQL server (only include important fields into independent LinkedList)
     * @param url
     */

    public void dbConnect(String url)
    {
        try {
            // read cclf1 data from sql server
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            this.conn = DriverManager.getConnection(url);
            Statement statement = this.conn.createStatement();
            String queryString = "SELECT t1.[BeneficiaryHIC_Number],t1.[CurrentClaimUniqueIdentifier], t1.[ProviderOSCAR_Number], t1.[ClaimFromDate] " +
                    ",t1.[ClaimThroughDate], t1.[BeneficiaryEquitableBIC_HICN_Number], t1.[ClaimAdjustmentTypeCode] FROM [ACODB].[ACO].[CCLF1] as t1 order by t1.[BeneficiaryHIC_Number]" ;

            ResultSet rs = statement.executeQuery(queryString);

            // read each column into a LinkedList
            while (rs.next()) {
                this.rRecs += 1;
                this.BeneficiaryHIC_Number.add(rs.getString(1));
                this.CurrentClaimUniqueIdentifier.add(rs.getString(2));
                this.ProviderOscar_Number.add(rs.getString(3));
                this.ClaimFromDate.add(rs.getString(4));
                this.ClaimThroughDate.add(rs.getString(5));
                this.BeneficiaryEquitableBIC_HICN_Number.add(rs.getString(6));
                this.ClaimAdjustmentTypeCode.add(rs.getString(7));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        // print results
        System.out.println(" ===== data loaded =====");
        System.out.printf(" The overall number of records is %d %n", this.rRecs);

    }


    /**
     * Insert claimUniqueIdentifier and DeletedReason to a new table
     * @param id
     * @param reason
     */


    public void insert(String id, String reason) {
        String sql = "INSERT INTO" + this.outputPath+ " (CurrentClaimUniqueIdentifier,Deleted) VALUES(?,?)";
        try (
            PreparedStatement pstmt = this.conn.prepareStatement(sql)) {
            pstmt.setString(1, id);
            pstmt.setString(2, reason);
            pstmt.executeUpdate();
        } catch (SQLException e) { System.out.println(e.getMessage()); }
    }


    /**
     * Create the output table and and insert each patient judification information to the table
     * @param url
     */


    public void CreateTableInSQL(String url) {
        System.out.println(" ====== output process ======");
        try {
            // connect to sql server
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            Connection conn = DriverManager.getConnection(url);
            System.out.println("connected");
            Statement statement = conn.createStatement();
            // drop the result table at very beginning
            String sql1 = "drop TABLE " + this.outputPath;
            statement.executeUpdate(sql1);
            // recreated the table
            String sql = "CREATE TABLE " + this.outputPath + " ("
                    + "CurrentClaimUniqueIdentifier         VARCHAR(255), "
                    + "Deleted                              VARCHAR(255) );";

            statement.executeUpdate(sql);
            // print the length of the deletion results
            System.out.println(this.DeleteClaim.size());
            System.out.println(this.ClaimDeleteReason.size());
            for (int i = 0; i < this.DeleteClaim.size(); ++i) {
                // insert the deletion results row by row
                insert(this.DeleteClaim.get(i), this.ClaimDeleteReason.get(i) );
            }
            System.out.println("Done");
        }

        catch (Exception e) {
            e.printStackTrace();
        }
    }



    // for each patient, find his/her own data from a giant input table
    public ArrayList<String[]> findAdjustedClaimsforAPatient(ArrayList<ArrayList<String>> rec) {
        ArrayList<String[]> deleted = new ArrayList<String[]>();
        ArrayList<String> deletedID = new ArrayList<String>();

        // loop over a patients all records row by row (deleted)
        for (int i = 0; i < rec.get(0).size(); ++i) {
            // if  ClaimAdjustmentTypeCode == 1
            if (rec.get(5).get(i).equals("1")) {
                // loop over this patients all records before the delete claim
                for (int j = 0; j < i; j++) {
                    // find a pair, if a previous claim has the same keys
                    if (rec.get(1).get(i).equals(rec.get(1).get(j)) && rec.get(2).get(i).equals(rec.get(2).get(j)) &&
                        rec.get(3).get(i).equals(rec.get(3).get(j)) && rec.get(4).get(i).equals(rec.get(4).get(j)))
                        {
                        // a related claim should be deleted as well, so add them into delete table
                        String[] d1 = new  String[] {rec.get(0).get(j), "dlt by dlt"};
                        deleted.add(d1);
                        deletedID.add(rec.get(0).get(j));

                        String[] d2 = new String[] {rec.get(0).get(i), "dlt"};
                        deleted.add(d2);
                        deletedID.add(rec.get(0).get(i));
                        }
                }


                for (int j = i; j < rec.get(0).size(); j++) {
                    // if a previous claim has the same keys
                    if (rec.get(5).get(j).equals("2") && !deletedID.contains(rec.get(0).get(i)) &&
                        rec.get(1).get(i).equals(rec.get(1).get(j)) && rec.get(2).get(i).equals(rec.get(2).get(j)) &&
                        rec.get(3).get(i).equals(rec.get(3).get(j)) && rec.get(4).get(i).equals(rec.get(4).get(j)))
                        {
                        // a related claim should be deleted as well, so add them into delete table
                        String[] d3 = new  String[] {rec.get(0).get(j), "adj after dlt"};
                        deleted.add(d3);
                        deletedID.add(rec.get(0).get(j));

                        String[] d4 = new String[] {rec.get(0).get(i), "dlt"};
                        deleted.add(d4);
                        deletedID.add(rec.get(0).get(i));
                        }
                }
                }
            }


        return deleted;
        }





    // create data structure for each patient
    public ArrayList<ArrayList<String>> createPatientProfile(String PatientID) {
        // local variables
        ArrayList<String> CurrentClaimUniqueIdentifier_ps = new ArrayList<>();
        ArrayList<String> ProviderOscar_Number_ps = new ArrayList<>();
        ArrayList<String> ClaimFromDate_ps = new ArrayList<>();
        ArrayList<String> ClaimThroughDate_ps = new ArrayList<>();
        ArrayList<String> BeneficiaryEquitableBIC_HICN_Number_ps = new ArrayList<>();
        ArrayList<String> ClaimAdjustmentTypeCode_ps = new ArrayList<>();
        ArrayList<ArrayList<String>> Recs_ps = new ArrayList<>(); // return this

        boolean tickerMove = true;
        while (tickerMove && this.BeneficiaryHIC_Number.size() != 0) {
            // This records belong to the same patient
            if (this.BeneficiaryHIC_Number.getFirst().equals(PatientID)) {
                // BeneficiaryHIC_Number
                // BeneficiaryHIC_Number_ps.add(this.BeneficiaryHIC_Number.getFirst());
                this.BeneficiaryHIC_Number.removeFirst();

                // CurrentClaimUniqueIdentifier
                CurrentClaimUniqueIdentifier_ps.add(this.CurrentClaimUniqueIdentifier.getFirst());
                this.CurrentClaimUniqueIdentifier.removeFirst();

                // ProviderOscarNumber
                ProviderOscar_Number_ps.add(this.ProviderOscar_Number.getFirst());
                this.ProviderOscar_Number.removeFirst();

                // ClaimFromDate
                ClaimFromDate_ps.add(this.ClaimFromDate.getFirst());
                this.ClaimFromDate.removeFirst();

                // ClaimThroughDate
                ClaimThroughDate_ps.add(this.ClaimThroughDate.getFirst());
                this.ClaimThroughDate.removeFirst();

                // BeneficiaryEquitableBIC_HICN_Number
                BeneficiaryEquitableBIC_HICN_Number_ps.add(this.BeneficiaryEquitableBIC_HICN_Number.getFirst());
                this.BeneficiaryEquitableBIC_HICN_Number.removeFirst();

                // ClaimAdjustmentTypeCode
                ClaimAdjustmentTypeCode_ps.add(this.ClaimAdjustmentTypeCode.getFirst());
                this.ClaimAdjustmentTypeCode.removeFirst();
            }
            else tickerMove = false;
        }
        Recs_ps.add(CurrentClaimUniqueIdentifier_ps );
        Recs_ps.add(ProviderOscar_Number_ps);
        Recs_ps.add(ClaimFromDate_ps);
        Recs_ps.add(ClaimThroughDate_ps);
        Recs_ps.add(BeneficiaryEquitableBIC_HICN_Number_ps);
        Recs_ps.add(ClaimAdjustmentTypeCode_ps);

        return Recs_ps;


    }


    // start adjudication
    public void cleanProcess() {
        System.out.println(" ====== clean process ======");
        LinkedList<String> IdList = new LinkedList<String>();
        LinkedList<String> ReasonList = new LinkedList<String>();

        Set<String> ps_set = new LinkedHashSet(this.BeneficiaryHIC_Number);
        System.out.println(ps_set.size());

        for (String ps: ps_set) {
            System.out.println(ps);
            ArrayList<ArrayList<String>> ps_data = createPatientProfile(ps);
            if (ps_data.get(0).size() == 0)
            {System.out.println("error: no data found");}
            else {
                ArrayList<String[]> ps_deleted = findAdjustedClaimsforAPatient(ps_data);
                for (int i = 0; i < ps_deleted.size(); ++i) {
                    if (!this.DeleteClaim.contains(ps_deleted.get(i)[0])) {
                        this.DeleteClaim.add(ps_deleted.get(i)[0]);
                        this.ClaimDeleteReason.add(ps_deleted.get(i)[1]);
                    }
                }
            }
        }
    }



    // running
    public void run() {
        String connectionUrl = "jdbc:sqlserver://DW-STAGE01; databaseName=ACODB;integratedSecurity=true;";
        dbConnect(connectionUrl);
        cleanProcess();
        CreateTableInSQL(connectionUrl);

    }



    public static void main(String[] args)
    {
        CCLF1_ADJUDICATION_NEW db  = new CCLF1_ADJUDICATION_NEW();
        db.run();
    }


}