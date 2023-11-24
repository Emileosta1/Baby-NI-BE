using System;
using System.Data;
using System.IO;
using Vertica.Data;
using Vertica.Data.VerticaClient;

namespace BabyNiProject
{
    public class FileLoader
    {
        private string connectionString; // Your Vertica database connection string

        public FileLoader(IConfiguration configuration)
        {
            connectionString = configuration.GetSection("DbSettings:VerticaConnectionString").Value;
        }

        public void LoadFilesToVertica(string parserDirectory)
        {
            using (var connection = new VerticaConnection(connectionString))
            {
                connection.Open();


                foreach (var filePath in Directory.GetFiles(parserDirectory, "*.csv"))
                {
                    string fileName = Path.GetFileName(filePath);

                    if (fileName.Contains("RADIO_LINK", StringComparison.OrdinalIgnoreCase))
                    {
                        ProcessTable(connection, parserDirectory, fileName, "RadioLink");
                    }
                    else if (fileName.Contains("RfInput", StringComparison.OrdinalIgnoreCase))
                    {
                        ProcessTable(connection, parserDirectory, fileName, "RfInput");
                    }
                }
            }
        }

        private void ProcessTable(VerticaConnection connection, string parserDirectory, string fileName, string tableName)
        {
            if (DoesTableExist(connection, tableName))
            {
                Console.WriteLine($"{tableName} table exists. Loading fully processed files to Vertica.");
                LoadProcessedFiles(connection, parserDirectory, tableName);
            }
            else
            {
                Console.WriteLine($"{tableName} table does not exist. Creating the table and then loading files to Vertica.");
                CreateTable(connection, tableName);
                LoadProcessedFiles(connection, parserDirectory, tableName);
            }
        }



        private bool DoesTableExist(VerticaConnection connection, string tableName)
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandText = "SELECT COUNT(*) FROM tables WHERE table_name = :tableName";
                command.Parameters.Add(new VerticaParameter(":tableName", tableName));
                int count = Convert.ToInt32(command.ExecuteScalar());
                return count > 0;
            }
        }

        private void CreateTable(VerticaConnection connection, string tableName)
        {
            using (var command = connection.CreateCommand())
            {
                // Define the schema and columns for the table based on the table name
                string columnDefinition = GetColumnDefinition(tableName);
                command.CommandText = $"CREATE TABLE {tableName} {columnDefinition}";
                command.ExecuteNonQuery();
            }
        }

        private string GetColumnDefinition(string tableName)
        {
            // Define different column definitions based on the table name
            if (tableName.Equals("RadioLink", StringComparison.OrdinalIgnoreCase))
            {
                return "(NETWORK_SID INT, DATETIME_KEY TIMESTAMP, NEID FLOAT, OBJECT VARCHAR(255),TIME TIMESTAMP, \"INTERVAL\" INT, DIRECTION VARCHAR(255), NEALIAS VARCHAR(255), NETYPE VARCHAR(255), RXLEVELBELOWTS1 INT, RXLEVELBELOWTS2 INT, MINRXLEVEL FLOAT, MAXRXLEVEL FLOAT, TXLEVELABOVETS1 INT, MINTXLEVEL FLOAT, MAXTXLEVEL FLOAT, FAILUREDESCRIPTION VARCHAR(255), LINK VARCHAR(255), TID VARCHAR(255), FARENDTID VARCHAR(255), SLOT INT, PORT INT)";
            }
            else if (tableName.Equals("RfInput", StringComparison.OrdinalIgnoreCase))
            {
                return "(NETWORK_SID INT, DATETIME_KEY TIMESTAMP, NODENAME VARCHAR(255), NEID INT, OBJECT VARCHAR(255), TIME TIMESTAMP, \"INTERVAL\" INT, DIRECTION VARCHAR(255), NEALIAS VARCHAR(255), NETYPE VARCHAR(255), RFINPUTPOWER FLOAT, TID VARCHAR(255), FARENDTID VARCHAR(255), SLOT VARCHAR(255), PORT INT)";
            }
            else
            {
                return "(NULL)";
            }


        }


        private void LoadProcessedFiles(VerticaConnection connection, string parserDirectory, string tableName)
        {
            var fileParser = new FileParser();
            string loaderDirectory = "C:\\Users\\User\\Desktop\\Project\\BabyNiProject\\Loader";

            foreach (var filePath in Directory.GetFiles(parserDirectory, "*.csv"))
            {
                try
                {
                    /*if (!File.Exists(filePath))
                    {
                        Console.WriteLine($"File not found: {filePath}");
                        continue;
                    }*/

                    string fileName = Path.GetFileName(filePath);

                    using (var command = connection.CreateCommand())
                    {
                        // Use parameterized query
                        command.CommandText = $"COPY {tableName} FROM LOCAL '{filePath}' DELIMITER ',' SKIP 1";

                        command.ExecuteNonQuery();
                        Console.WriteLine($"Loaded {fileName} to Vertica table '{tableName}'.");

                        // Move the file to the loader directory
                        string destinationPath = Path.Combine(loaderDirectory, fileName);
                        File.Move(filePath, destinationPath);
                        Console.WriteLine($"Moved {fileName} to {loaderDirectory}.");
                    }
                }
                catch (Exception ex)
                {
                    // Log the exception
                    Console.WriteLine($"Error loading file {filePath}: {ex.Message}");
                }
            }
        }
    }
}