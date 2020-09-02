/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package bulkload;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;

/**
 * Usage: java bulkload.BulkLoad
 */
public class BulkLoad
{
    //public static final String CSV_URL = "https://sdc-bucket.s3-us-west-1.amazonaws.com/yahoo.csv";

    /** Default output directory */
    public static final String DEFAULT_OUTPUT_DIR = "./data";

    public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

    /** Keyspace name */
    public static final String KEYSPACE = "calendar";//TODO set keyspace name
    /** Table name */
    public static final String TABLE = "reservations_by_room";//TODO set table name

    /**
     * Schema for bulk loading table.
     * It is important not to forget adding keyspace name before table name,
     * otherwise CQLSSTableWriter throws exception.
     */
    //TODO set schema
    public static final String SCHEMA = String.format("CREATE TABLE %s.%s (" +
                                                          "id int, " +
                                                          "check_in timestamp, " +
                                                          "check_out timestamp, " +
                                                          "guests int, " +
                                                          "room_id int, " +
                                                          "user_id int, " +
                                                          "PRIMARY KEY (room_id, id) " +
                                                          ") WITH CLUSTERING ORDER BY (id DESC)", KEYSPACE, TABLE);

    /**
     * INSERT statement to bulk load.
     * It is like prepared statement. You fill in place holder for each data.
     */
    //TODO set insert with columns
    public static final String INSERT_STMT = String.format("INSERT INTO %s.%s (" +
                                                               "id,check_in,check_out,guests,room_id,user_id" +
                                                           ") VALUES (" +
                                                               "?, ?, ?, ?, ?, ?" +
                                                           ")", KEYSPACE, TABLE);

    public static void main(String[] args)
    {
        // magic!
        Config.setClientMode(true);

        // Create output directory that has keyspace and table name in the path
        File outputDir = new File(DEFAULT_OUTPUT_DIR + File.separator + KEYSPACE + File.separator + TABLE);
        if (!outputDir.exists() && !outputDir.mkdirs())
        {
            throw new RuntimeException("Cannot create output directory: " + outputDir);
        }

        // Prepare SSTable writer
        CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder();
        // set output directory
        builder.inDirectory(outputDir)
               // set target schema
               .forTable(SCHEMA)
               // set CQL statement to put data
               .using(INSERT_STMT)
               // set partitioner if needed
               // default is Murmur3Partitioner so set if you use different one.
               .withPartitioner(new Murmur3Partitioner());
        CQLSSTableWriter writer = builder.build();

            try (
                InputStream inputStream = new FileInputStream("/Users/blancagomez/Github/SEI/calendar/server/csv/reservations.csv"); //TODO set path to file
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                CsvListReader csvReader = new CsvListReader(reader, CsvPreference.STANDARD_PREFERENCE)
            )
            {

                csvReader.getHeader(true);

                // Write to SSTable while reading data
                List<String> line;
                while ((line = csvReader.read()) != null)
                {
                    // We use Java types here based on
                    // http://www.datastax.com/drivers/java/2.0/com/datastax/driver/core/DataType.Name.html#asJavaClass%28%29
                    //TODO set types for columns
                    writer.addRow(Integer.parseInt(line.get(0)),
                                  DATE_FORMAT.parse(line.get(1)),
                                  DATE_FORMAT.parse(line.get(2)),
                                  Integer.parseInt(line.get(3)),
                                  Integer.parseInt(line.get(4)),
                                  Integer.parseInt(line.get(5)));
                }
            }
            catch (InvalidRequestException | ParseException | IOException e)
            {
                e.printStackTrace();
            }

        try
        {
            writer.close();
        }
        catch (IOException ignore) {}
    }
}
