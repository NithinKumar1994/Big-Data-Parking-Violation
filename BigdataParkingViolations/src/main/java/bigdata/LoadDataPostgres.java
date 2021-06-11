package bigdata; /**
 * Big Data project
 * @author Nithin Kumar Pechetti np2598
 */


import javax.swing.*;
import java.awt.*;
import java.io.*;
import java.sql.*;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * CLass to load parking Violations tables
 */
public class LoadDataPostgres {
    private static final Logger LOGGER = Logger.getLogger(LoadDataPostgres.class.getName());
    static final String URL = "jdbc:postgresql://localhost:5432/postgres";
    static final String USERNAME = "postgres";
    static final String PASSWORD = "Nithin_4567";
    static final File CONFIG_FILE = new File("src/main/resources/application.properties");
    static BufferedWriter bwVehicle;
    static BufferedWriter bwPolice;
    static BufferedWriter bwStreet;
    static BufferedWriter bwTicket;

    public LoadDataPostgres() {
        dropRecords();
        createFiles();
    }

    /**
     * Function that returns the connection to Postgres Database
     * @return Connection class object
     * @throws SQLException Catches any exceptions in SQL
     */
    public Connection connect() throws SQLException {
        return DriverManager.getConnection(URL, USERNAME, PASSWORD);
    }

    /**
     * Function used to drop records if it exists inorder to load all tables again
     */
    public void dropRecords() {
        String[] tables = {"StreetInformationtemp", "ParkingViolation", "StreetInformation", "Policetemp", "Police",
                "Vehicletemp", "Vehicle", "ViolationCode", "parkingviolationtemp"};
        try (Connection con = connect()) {
            for (String table : tables) {
                con.prepareStatement("DROP TABLE IF EXISTS public.\"" + table + "\";").execute();
            }
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Error occured in SQL execution", e);
        }
    }

    /**
     * Function used to create files and Buffered writers
     * to write data into the respective files. These files are then used to load the resepective tables.
     */
    public static void createFiles() {

        try (FileReader reader = new FileReader(CONFIG_FILE)) {

            Properties props = new Properties();
            props.load(reader);

            File vehicleFile = new File(props.getProperty("vehicle.filepath"));
            File policeFile = new File(props.getProperty("police.filepath"));
            File streetFile = new File(props.getProperty("street.filepath"));
            File ticketFile = new File(props.getProperty("ticket.filepath"));

            bwVehicle = new BufferedWriter(new FileWriter(vehicleFile));
            bwPolice = new BufferedWriter(new FileWriter(policeFile));
            bwStreet = new BufferedWriter(new FileWriter(streetFile));
            bwTicket = new BufferedWriter(new FileWriter(ticketFile));
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Error occured in creating files method", e);
        }

    }

    /**
     * Function used to write data into different files from the data source.
     * @param filename Filename including its path
     * @throws IOException
     */
    public void writeData(String filename) throws IOException {
        String s;

        File file = new File(filename);
        try (Connection con = connect(); BufferedReader br = new BufferedReader(new FileReader(file))) {
            con.setAutoCommit(true);
            br.readLine();//skip header
            while ((s = br.readLine()) != null && s.length() != 0) {
                String[] line = s.split(",", -1);
                if (line.length == 43){
                    if (!(line[16].equals("0") || line[16].equals("") || line[17].equals("0") || line[17].equals(""))) {
                        bwPolice.write(line[16] + "," + line[17] + "," + line[18] + "," + line[15] + "," + line[8] + "\n");
                    }
                    bwStreet.write(Integer.parseInt(line[9]) + "," + Integer.parseInt(line[10]) + "," + Integer.parseInt(line[11]) + "," + line[24] + "\n");
                    bwVehicle.write(line[1] + "," + (line[6].equals("") ? "NULL" : line[6]) + "," + line[3] + "," + (line[7].equals("") ? "NULL" : line[7]) + "," + (line[12].equals("0") || line[12].contains("88") || line[12].equals("") ? "NULL" : line[12].substring(0, 4) + "-" + line[12].substring(4, 6) + "-" + line[12].substring(6, 8)) + "," + (line[33].equals("") ? "NULL" : line[33]) + "," + (line[34].equals("") ? "NULL" : line[34]) + "," + (line[35].equals("0") ? "NULL" : line[35]) + "\n");
                    for (int n = 0; n < line.length; n++) {
                        if (line[n].equals("")) {
                            line[n] = "NULL";
                        }
                    }
                    bwTicket.write((line[0].equals("") ? "NULL" : line[0]) + "\t" + (line[1].equals("") ? "NULL" : line[1]) + "\t" + (line[5].equals("") ? "NULL" : line[5]) + "\t" + (line[9].equals("") ? "NULL" : line[9]) + "\t" + (line[10].equals("") ? "NULL" : line[10]) + "\t" + (line[11].equals("") ? "NULL" : line[11]) + "\t" + (line[24].equals("") ? "NULL" : line[24]) + "\t" + (line[13].equals("") ? "NULL" : line[13]) + "\t" + (line[14].equals("") ? "NULL" : line[14]) + "\t" + (line[19].equals("") ? "NULL" : line[19]) + "\t" + (line[21].equals("") ? "NULL" : line[21]) + "\t" + ((line[16].equals("")|| line[16].equals("0")) ? "NULL" : line[16]) + "\t" + (line[37].equals("") ? "NULL" : line[37]) + "\t" + (line[25].equals("") ? "NULL" : line[25]) + "\t" + (line[26].equals("") ? "NULL" : line[26]) + "\t" + line[20] + "\t" + line[23] + "\n");
                }
            }

        } catch (SQLException | IOException e) {
            LOGGER.log(Level.SEVERE, "Error occured in loading files", e);
        }
        finally {
            bwPolice.close();
            bwStreet.close();
            bwTicket.close();
            bwVehicle.close();
        }
    }

    public static void main(String[] args) throws IOException {

        LoadDataPostgres load = new LoadDataPostgres();
        final JFrame frame = new JFrame("JDialog Demo");
        final JButton btnLogin = new JButton("Click to login");

        btnLogin.addActionListener(
                e -> {
                    DatabaseLogin dbDlg = new DatabaseLogin(frame);
                    dbDlg.setVisible(true);
                    // if logon successfully
                    if(dbDlg.isSucceeded()){
                        btnLogin.setText("Hi " + dbDlg.getUsername() + "!");
                    }
                });

        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setSize(300, 100);
        frame.setLayout(new FlowLayout());
        frame.getContentPane().add(btnLogin);
        frame.setVisible(true);
        try (Connection con = DriverManager.getConnection(URL, USERNAME, PASSWORD)){
            LOGGER.info("Creating data files");
            load.writeData("C:\\\\temp\\Parking_Violations_Issued_-_Fiscal_Year_2017.csv");
            LOGGER.info("Creating tables");

            try (FileReader reader = new FileReader(CONFIG_FILE)) {
                Properties props = new Properties();
                props.load(reader);
                try(BufferedReader br=new BufferedReader(new FileReader(props.getProperty("streetInformation.filepath")))){
                    String s="";
                    StringBuilder sb=new StringBuilder();
                    while((s=br.readLine())!=null){
                        sb.append(s);
                    }
                    System.out.println("executing");
                    con.prepareStatement(sb.toString()).execute();
                    LOGGER.info("Loading Data completed");
                }
                catch(SQLException e){
                    LOGGER.log(Level.SEVERE,"SQL Exception",e);
                }
            }
            catch(Exception e){
                LOGGER.log(Level.SEVERE, "Error occured", e);
            }
        } catch (SQLException | IOException e) {
            LOGGER.log(Level.SEVERE, "Error occured in SQL execution", e);
        }
    }
}

