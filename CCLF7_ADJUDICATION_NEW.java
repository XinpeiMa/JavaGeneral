package MyCode;

import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Set;

public class CCLF7_ADJUDICATION_NEW {
    // input fields
    private LinkedList<String> CurrentClaimUniqueIdentifier = new LinkedList<String>();
    private LinkedList<String> BeneficiaryHIC_Number = new LinkedList<String>();
    private LinkedList<String> ClaimAdjustmentTypeCode = new LinkedList<String>();
    private LinkedList<String> ClaimEffectiveDate = new LinkedList<String>();
    private LinkedList<String> ClaimLineFromDate = new LinkedList<String>();
    private LinkedList<String> ProviderServiceIdentifierQualifierCode = new LinkedList<String>();
    private LinkedList<String> ClaimServiceProviderGenericID_Number = new LinkedList<String>();
    private LinkedList<String> ClaimDispensingStatusCode = new LinkedList<String>();
    private LinkedList<String> ClaimLinePrescriptionServiceReferenceNumber = new LinkedList<String>();
    private LinkedList<String> ClaimLinePrescriptionFillNumber = new LinkedList<String>();
    private String outputPath = "[ACODB].[xma].[TestCCLF7_part2]";

    // output fields
    private LinkedList<String> DeleteClaim = new LinkedList<String>();
    private LinkedList<String> ClaimDeleteReason = new LinkedList<String>();
    private Connection conn;

    // the number of records
    private int rRecs = 0;

    public void dbConnect(String url)
    {
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            this.conn = DriverManager.getConnection(url);
            Statement statement = this.conn.createStatement();
            String queryString = " SELECT distinct t1.[CurrentClaimUniqueIdentifier], t1.[BeneficiaryHIC_Number], t1.[ClaimAdjustmentTypeCode], t1.[ClaimEffectiveDate] " +
                    ",t1.[ClaimLineFromDate], t1.[ProviderServiceIdentifierQualifierCode], t1.[ClaimServiceProviderGenericID_Number], t1.[ClaimDispensingStatusCode], " +
                    " t1.[ClaimLinePrescriptionServiceReferenceNumber], t1.[ClaimLinePrescriptionFillNumber] FROM [ACODB].[xma].[CCLF7] t1 " +
                    " inner join [ACODB].[xma].[TestCCLF7_Split] t2 on t1.[BeneficiaryHIC_Number] = t2.[BeneficiaryHIC_Number]" +
                    " where t2.[RN] > 5000000" +
                    " order by t1.[BeneficiaryHIC_Number], t1.[ClaimEffectiveDate], t1.[ClaimAdjustmentTypeCode]";

            ResultSet rs = statement.executeQuery(queryString);
            while (rs.next()) {
                this.rRecs += 1;
                this.CurrentClaimUniqueIdentifier.add(rs.getString(1));
                this.BeneficiaryHIC_Number.add(rs.getString(2));
                this.ClaimAdjustmentTypeCode.add(rs.getString(3));
                this.ClaimEffectiveDate.add(rs.getString(4));
                this.ClaimLineFromDate.add(rs.getString(5));
                this.ProviderServiceIdentifierQualifierCode.add(rs.getString(6));
                this.ClaimServiceProviderGenericID_Number.add(rs.getString(7));
                this.ClaimDispensingStatusCode.add(rs.getString(8));
                this.ClaimLinePrescriptionServiceReferenceNumber.add(rs.getString(9) );
                this.ClaimLinePrescriptionFillNumber.add(rs.getString(10));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println(" ===== data loaded =====");
        System.out.printf(" The overall number of records is %d %n", this.rRecs);

    }


    public void insert(String id, String reason) {
        String sql = "INSERT INTO " + this.outputPath + " (CurrentClaimUniqueIdentifier,Deleted) VALUES(?,?)";
        try (
            PreparedStatement pstmt = this.conn.prepareStatement(sql)) {
            pstmt.setString(1, id);
            pstmt.setString(2, reason);
            pstmt.executeUpdate();

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }


    public void CreateTableInSQL(String url) {
        System.out.println(" ====== output process ======");
        try {

            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            Connection conn = DriverManager.getConnection(url);
            System.out.println("connected");
            Statement statement = conn.createStatement();
            // drop the table at very beginning
            String sql1 = "drop TABLE  " + this.outputPath;
            statement.executeUpdate(sql1);
            // recreated the table
            String sql = "CREATE TABLE " + this.outputPath + "      ("
                    + "CurrentClaimUniqueIdentifier         VARCHAR(255), "
                    + "Deleted                              VARCHAR(255) );";

            statement.executeUpdate(sql);

            System.out.println(this.DeleteClaim.size());
            System.out.println(this.ClaimDeleteReason.size());
            for (int i = 0; i < this.DeleteClaim.size(); ++i) {
                insert(this.DeleteClaim.get(i), this.ClaimDeleteReason.get(i) );

            }
            System.out.println("Done");
        }

        catch (Exception e) {
            e.printStackTrace();
        }
    }




    public ArrayList<String[]>  findAdjustedClaimsforAPatient(ArrayList<ArrayList<String>> rec) {
        ArrayList<String[]> deleted = new ArrayList<String[]>();
        ArrayList<String> deletedID = new ArrayList<String>();
        for (int i = 0; i < rec.get(0).size(); ++i) {

            if (rec.get(1).get(i).equals("1")) {

                for (int j = 0; j < i; j++) {
                    // index 2, 3, 4, 5, 6, 7
                    if (rec.get(2).get(i).equals(rec.get(2).get(j)) && rec.get(3).get(i).equals(rec.get(3).get(j)) &&
                            rec.get(4).get(i).equals(rec.get(4).get(j)) && rec.get(5).get(i).equals(rec.get(5).get(j)) &&
                            rec.get(6).get(i).equals(rec.get(6).get(j)) && rec.get(7).get(i).equals(rec.get(7).get(j)) )
                    {
                        // a related claim should be deleted as well, so add them into delete table
                        String[] d1 = new String[] {rec.get(0).get(j), "dlt by dlt"};
                        deleted.add(d1);
                        deletedID.add(rec.get(0).get(j));

                        String[] d2 = new String[] {rec.get(0).get(i), "dlt"};
                        deleted.add(d2);
                        deletedID.add(rec.get(0).get(i));
                    }
                }

                for (int j = i; j < rec.get(0).size(); j++) {
                    // use index 1 and index 2
                    if (rec.get(1).get(j).equals("2") && !deletedID.contains(rec.get(0).get(i)) &&
                            rec.get(2).get(i).equals(rec.get(2).get(j)) && rec.get(3).get(i).equals(rec.get(3).get(j)) &&
                            rec.get(4).get(i).equals(rec.get(4).get(j)) && rec.get(5).get(i).equals(rec.get(5).get(j)) &&
                            rec.get(6).get(i).equals(rec.get(6).get(j)) && rec.get(7).get(i).equals(rec.get(7).get(j)) )
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


    public ArrayList<ArrayList<String>> createPatientProfile(String PatientID) {
        ArrayList<String> CurrentClaimUiqueIdentifier_ps = new ArrayList<String>();
        ArrayList<String> ClaimAdjustmentTypeCode_ps = new ArrayList<String>();
        ArrayList<String> ClaimLineFromDate_ps = new ArrayList<String>();
        ArrayList<String> ProviderServiceIdentifierQualifierCode_ps = new ArrayList<String>();
        ArrayList<String> ClaimServiceProviderGenericID_Number_ps = new ArrayList<String>();
        ArrayList<String> ClaimDispensingStatusCode_ps = new ArrayList<String>();
        ArrayList<String> ClaimLinePrescriptionServiceReferenceNumber_ps = new ArrayList<String>();
        ArrayList<String> ClaimLinePrescriptionFillNumber_ps = new ArrayList<String>();

        ArrayList<ArrayList<String>> Recs_ps = new ArrayList<ArrayList<String>>();

        boolean tickerMove = true;
        while (tickerMove && this.BeneficiaryHIC_Number.size() != 0) {
            if (this.BeneficiaryHIC_Number.getFirst().equals(PatientID)) {

                // CurrentClaimUniqueIdentifier, index 0
                CurrentClaimUiqueIdentifier_ps.add(this.CurrentClaimUniqueIdentifier.getFirst());
                this.CurrentClaimUniqueIdentifier.removeFirst();

                // BeneficiaryHIC_Number
                //BeneficiaryHIC_Number_ps.add(this.BeneficiaryHIC_Number.getFirst());
                this.BeneficiaryHIC_Number.removeFirst();

                // ClaimAdjustmentTypeCode, index 1
                ClaimAdjustmentTypeCode_ps.add(this.ClaimAdjustmentTypeCode.getFirst());
                this.ClaimAdjustmentTypeCode.removeFirst();

                // ClaimLineFromDate, index 2
                ClaimLineFromDate_ps.add(this.ClaimLineFromDate.getFirst());
                this.ClaimLineFromDate.removeFirst();

                //ProviderServiceIdentifierQualifierCode, index 3
                ProviderServiceIdentifierQualifierCode_ps.add(this.ProviderServiceIdentifierQualifierCode.getFirst());
                this.ProviderServiceIdentifierQualifierCode.removeFirst();


                //ProviderServiceIdentifierQualifierCode, index 4
                ClaimServiceProviderGenericID_Number_ps.add(this.ClaimServiceProviderGenericID_Number.getFirst());
                this.ClaimServiceProviderGenericID_Number.removeFirst();


                //  ClaimDispensingStatusCode, index 5
                ClaimDispensingStatusCode_ps.add(this.ClaimDispensingStatusCode.getFirst());
                this.ClaimDispensingStatusCode.removeFirst();

                // ClaimLinePrescriptionServiceReferenceNumber, index 6
                ClaimLinePrescriptionServiceReferenceNumber_ps.add(this.ClaimLinePrescriptionServiceReferenceNumber.getFirst());
                this.ClaimLinePrescriptionServiceReferenceNumber.removeFirst();

                // ClaimLinePrescriptionFillNumber, index 7
                ClaimLinePrescriptionFillNumber_ps.add(this.ClaimLinePrescriptionFillNumber.getFirst());
                this.ClaimLinePrescriptionFillNumber.removeFirst();




            }
            else tickerMove = false;
        }


        Recs_ps.add(CurrentClaimUiqueIdentifier_ps );
        Recs_ps.add(ClaimAdjustmentTypeCode_ps);
        Recs_ps.add(ClaimLineFromDate_ps);
        Recs_ps.add(ProviderServiceIdentifierQualifierCode_ps);
        Recs_ps.add(ClaimServiceProviderGenericID_Number_ps);
        Recs_ps.add(ClaimDispensingStatusCode_ps);
        Recs_ps.add(ClaimLinePrescriptionServiceReferenceNumber_ps);
        Recs_ps.add(ClaimLinePrescriptionFillNumber_ps);
        return Recs_ps;


    }




    public void cleanProcess() {
        System.out.println(" ====== clean process ===");
        LinkedList<String> IdList = new LinkedList<String>();
        LinkedList<String> ReasonList = new LinkedList<String>();

        Set<String> ps_set = new LinkedHashSet(this.BeneficiaryHIC_Number);
        System.out.println(ps_set.size());

        for (String ps: ps_set) {
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

    public void run() {
        String connectionUrl = "jdbc:sqlserver://DW-STAGE01; databaseName=ACODB;integratedSecurity=true;";
        dbConnect(connectionUrl);
        cleanProcess();
        CreateTableInSQL(connectionUrl);
    }



    public static void main(String[] args)
    {
        CCLF7_ADJUDICATION_NEW db  = new CCLF7_ADJUDICATION_NEW();
        db.run();
    }

}
