package bigdata;

import java.io.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import com.mongodb.client.MongoCollection;


public class LoadDataMongo {
        private static final Logger LOGGER = Logger.getLogger(LoadDataMongo.class.getName());
        static List<Long> duplicateIds=new ArrayList<>();
        static final File CONFIG=new File("src/main/resources/mongo.properties");
    /**
     * Function to load the Parking Violation data into MongoDB
     * @param file csv File which contains all the data
     * @param pvCollection The parking Violation to which the data needs to be loaded.
     * @throws IOException
     */
    public static void loadData(File file,MongoCollection<Document> pvCollection) throws IOException {
        duplicateIds.clear();
        var s="";
        try(BufferedReader br=new BufferedReader(new FileReader(file));) {
            br.lines().skip(1);
            while ((s = br.readLine()) != null && s.length() != 0) {
                var pvDoc = new Document();
                var vehicleDoc = new Document();
                var streetDoc = new Document();
                var issuerDoc = new Document();
                String[] line = s.split(",", -1);
                Long id = Long.parseLong(line[0]);
                if (line.length != 43 || duplicateIds.contains(id)) continue;//skipping line if it doesnt match the no. of columns
                if (!line[0].equals(""))
                    pvDoc.append("_id", id);
                duplicateIds.add(id);
                //vehicle information
                if (!line[1].equals(""))
                    vehicleDoc.append("plateId", line[1]);
                if (!line[6].equals(""))
                    vehicleDoc.append("BodyType", line[6]);
                if (!line[3].equals(""))
                    vehicleDoc.append("PlateType", line[3]);
                if (!line[7].equals(""))
                    vehicleDoc.append("Vehicle Make", line[7]);
                if (!line[12].contains("88") && !line[12].equals("") && !line[12].equals("0"))
                    vehicleDoc.append("ExpireDate", line[12].substring(0, 4) + "-" + line[12].substring(4, 6) + "-" + line[12].substring(6, 8));
                if (!line[33].equals(""))
                    vehicleDoc.append("VehicleColor", line[33]);
                if (!line[34].equals(""))
                    vehicleDoc.append("Unregistered Vehicle", line[34]);
                if (!line[35].equals("0"))
                    vehicleDoc.append("VehicleYear", line[35]);
                if (vehicleDoc.size() != 0)
                    pvDoc.append("vehicle", vehicleDoc);

                if (!line[5].equals(""))
                    pvDoc.append("ViolationCode", Integer.parseInt(line[5]));

                //street Information
                if (!line[9].equals(""))
                    streetDoc.append("Street Code 1", Integer.parseInt(line[9]));
                if (!line[10].equals(""))
                    streetDoc.append("Street Code 2", Integer.parseInt(line[10]));
                if (!line[11].equals(""))
                    streetDoc.append("Street Code 3", Integer.parseInt(line[11]));
                if (!line[24].equals(""))
                    streetDoc.append("Name", line[24]);
                if (streetDoc.size() != 0)
                    pvDoc.append("Street", streetDoc);


                if (!line[13].equals(""))
                    pvDoc.append("Location", line[13]);
                if (!line[14].equals(""))
                    pvDoc.append("precint", Integer.parseInt(line[14]));
                if (!line[19].equals("")) {
                    pvDoc.append("time", line[19]);
                }
                if (!line[21].equals("")) {
                    pvDoc.append("county", line[21]);
                }

                //issuer data stored in a document and appended to the collection.
                if (!line[16].equals(""))
                    issuerDoc.append("issuerId", Integer.parseInt(line[16]));
                if (!line[17].equals(""))
                    issuerDoc.append("Command", line[17]);
                if (!line[18].equals(""))
                    issuerDoc.append("Squad", line[18]);
                if (!line[15].equals(""))
                    issuerDoc.append("Precinct", Integer.parseInt(line[15]));
                if (!line[8].equals(""))
                    issuerDoc.append("Agency", line[8]);
                if (issuerDoc.size() != 0)
                    pvDoc.append("issuer", issuerDoc);
                if (!line[37].equals(""))
                    pvDoc.append("feetFromCurb", Integer.parseInt(line[37]));
                if (!line[25].equals(""))
                    pvDoc.append("IntersectingStreet", line[25]);
                if (!line[26].equals(""))
                    pvDoc.append("datefirstObserved", line[26]);
                if (!line[20].equals(""))
                    pvDoc.append("timefirstObserved", line[20]);
                if (!line[23].equals(""))
                    pvDoc.append("houseNumber", line[23]);

                pvCollection.insertOne(pvDoc);
            }
        }
        catch(Exception e) {
            LOGGER.log(Level.SEVERE,"Exception occured in function loadData",e);
        }
    }

    /**
     * Function to load violation collection
     * @param db database name
     * @param prop properties object
     */
    public static void loadViolaton(MongoDatabase db,Properties prop)  {
        MongoCollection<Document> violationColl = db.getCollection("Violations");
        File violationFile=new File(prop.getProperty("violation.filepath"));

        String violation;
        try(BufferedReader brViolation= new BufferedReader(new FileReader(violationFile));) {
            brViolation.lines().skip(1);//skipping the first line as it contains column names
            while ((violation = brViolation.readLine()) != null) {
                String[] line = violation.split(",");
                var violationDoc = new Document();
                violationDoc.append("_id", line[0]);
                violationDoc.append("description", line[1]);
                violationColl.insertOne(violationDoc);
            }
        }
        catch(IOException e){
                LOGGER.log(Level.SEVERE,"IO exception",e);
            }

    }

    public static void main(String[] args) {
        try (FileReader reader = new FileReader(CONFIG);) {

            Properties props = new Properties();
            props.load(reader);

            try (MongoClient mongoClient = new MongoClient("localhost", Integer.parseInt(props.getProperty("port")));) {
                LOGGER.info("MongoDB Server connection established");
                MongoDatabase db = mongoClient.getDatabase(props.getProperty("database"));
                LOGGER.info("Connected to database");
                LOGGER.info("Database name" + db.getName());
                MongoCollection<Document> pvCollection = db.getCollection(props.getProperty("collection"));

                int i = 2015;
                while (i < 2016) {
                    File file = new File(props.getProperty("parking.filepath") + i + ".csv");
                    loadData(file, pvCollection);
                    i++;
                }

                //Violation Collection which contains violation id and its description.
                loadViolaton(db,props);


            } catch (IOException e) {
                LOGGER.log(Level.SEVERE, "File not found", e);
            }
        }
        catch(IOException e){
            LOGGER.log(Level.SEVERE, "File not found", e);
        }
    }


    }

