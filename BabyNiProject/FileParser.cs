using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Security.Cryptography;
using System.Text;

namespace BabyNiProject
{
    public class FileParser
    {
        public void ConvertToCsv(string filePath)
        {
            if (File.Exists(filePath) && Path.GetExtension(filePath).Equals(".txt", StringComparison.OrdinalIgnoreCase))
            {
                try
                {
                    // Convert the file to .csv and get the new CSV file path
                    string csvFilePath = Path.ChangeExtension(filePath, ".csv");
                    File.Move(filePath, csvFilePath);
                    Console.WriteLine($"Converted {Path.GetFileName(filePath)} to CSV.");

                    // Read the CSV file into a list of dictionaries
                    List<Dictionary<string, string>> data = ReadCsvFile(csvFilePath);

                    // Check if the file name contains specific keywords
                    string fileName = Path.GetFileName(filePath);
                    if (fileName.Contains("RADIO_LINK_POWER"))
                    {
                        ModifyDataForRadioLinkPower(data, fileName);
                    }
                    else if (fileName.Contains("RFInputPower"))
                    {
                        ModifyDataForRFInputPower(data, fileName);
                    }

                    // Write the modified data back to the CSV file
                    WriteCsvFile(csvFilePath, data);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error converting to CSV: {ex.Message}");
                }
            }
        }


        private string CalculateSHA256Hash(string input)
        {
            using (SHA256 sha256 = SHA256.Create())
            {
                byte[] bytes = Encoding.UTF8.GetBytes(input);
                byte[] hashBytes = sha256.ComputeHash(bytes);

                // Convert the byte array to a hexadecimal string
                StringBuilder builder = new StringBuilder();
                for (int i = 0; i < hashBytes.Length; i++)
                {
                    builder.Append(hashBytes[i].ToString("x2"));
                }

                return builder.ToString();
            }
        }


        private void ModifyDataForRadioLinkPower(List<Dictionary<string, string>> data, string fileName)
        {
            // Apply modifications specific to files containing "RADIO_LINK_POWER" in the name
            // Create a list of rows to delete
            List<Dictionary<string, string>> rowsToDelete = new List<Dictionary<string, string>>();

            foreach (var row in data)
            {
                // Check the "object" column for the value "Unreachable bulk FC"
                if ((row.ContainsKey("Object") && row["Object"] == "Unreachable Bulk FC") || (row.ContainsKey("FailureDescription") && row["FailureDescription"] != "-"))
                {
                    // Add the row to the list of rows to delete
                    rowsToDelete.Add(row);
                }

                // Add or modify other columns as needed
                // row["new_column_radio"] = "new_value_for_radio"; // Add a new column
                row.Remove("NodeName"); // Delete a specific column
                row.Remove("Position");
                row.Remove("IdLogNum");
            }

            // Delete the rows marked for deletion
            foreach (var rowToDelete in rowsToDelete)
            {
                data.Remove(rowToDelete);
            }

            // Call the combined method to add the "NETWORK_SID" and "DATETIME_KEY" columns
            AddNetworkSidAndDatetimeKeyColumns(data, fileName);

            AddLinkColumn(data);
            AddTidAndFarendTidColumns(data);
            AddSlotAndPortColumns(data);

        }

        private void AddNetworkSidAndDatetimeKeyColumns(List<Dictionary<string, string>> data, string fileName)
        {
            // Extract the datetime information from the file name
            string dateTimePart = fileName.Substring(fileName.Length - 19, 15); //N

            // Convert the datetime to the desired format (dd-MM-yyyy HH:mm:ss)
            if (DateTime.TryParseExact(dateTimePart, "yyyyMMdd_HHmmss", null, DateTimeStyles.None, out DateTime dateTime))
            {
                string formattedDateTime = dateTime.ToString("dd/MM/yyyy HH:mm:ss");
                DateTime test = DateTime.ParseExact(formattedDateTime, "dd/MM/yyyy HH:mm:ss", null);

                foreach (var row in data)
                {
                    // Calculate the hash value of "NeAlias" and "NeType"
                    if (row.ContainsKey("NeAlias") && row.ContainsKey("NeType"))
                    {
                        string neAlias = row["NeAlias"];
                        string neType = row["NeType"];

                        // Calculate the hash value of NeAlias and NeType
                        string networkSid = CalculateSHA256Hash(neAlias + neType);

                        // Create a new dictionary for the row with "NETWORK_SID" at the beginning
                        var newRow = new Dictionary<string, string>
                {
                    { "NETWORK_SID", Math.Abs( networkSid .GetHashCode()).ToString() },
                    { "DATETIME_KEY", test.ToString() }
                };

                        // Copy the existing columns to the new dictionary
                        foreach (var kvp in row)
                        {
                            newRow[kvp.Key] = kvp.Value;
                        }

                        // Update the row with the new dictionary
                        row.Clear();
                        foreach (var kvp in newRow)
                        {
                            row[kvp.Key] = kvp.Value;
                        }
                    }
                }
            }
        }

        private void AddLinkColumn(List<Dictionary<string, string>> data)
        {
            foreach (var row in data)
            {
                if (row.ContainsKey("Object"))
                {
                    string objectValue = row["Object"];
                    string[] parts = objectValue.Split('_');

                    if (parts.Length > 0)
                    {
                        string linkValue = parts[0];

                        // Check if "." exists in the middle
                        if (linkValue.Contains("."))
                        {
                            // Split by "."
                            string[] slotPort = linkValue.Split('.');

                            if (slotPort.Length == 2)
                            {
                                // Case: "." exists in the middle
                                linkValue = $"{slotPort[0].Split('/').Last()}/{slotPort[1].Split('/').First()}";

                            }
                        }
                        else
                        {
                            string[] slotPort = linkValue.Split("/");
                            // Case: Neither "." nor "+" exists in the middle
                            linkValue = $"{slotPort[1]}/{slotPort[2]}";

                        }
                        row["LINK"] = linkValue;
                    }
                }
            }
        }

        public void AddTidAndFarendTidColumns(List<Dictionary<string, string>> data)
        {
            foreach (var row in data)
            {
                if (row.ContainsKey("Object"))
                {
                    string objectValue = row["Object"];
                    string[] parts = objectValue.Split('_');

                    string tidValue = parts[2];
                    string farendTidValue = parts[4];

                    row["TID"] = tidValue;
                    row["FARENDTID"] = farendTidValue;

                }
            }
        }



        private void AddSlotAndPortColumns(List<Dictionary<string, string>> data)
        {
            foreach (var row in data.ToList()) // Create a copy of the list to avoid concurrent modification
            {
                if (row.ContainsKey("LINK"))
                {
                    string linkValue = row["LINK"];

                    if (linkValue.Contains("+"))
                    {
                        string[] parts = linkValue.Split('/');

                        if (parts.Length == 2)
                        {
                            // Process the first part of the link
                            string slotPort1 = parts[0];
                            string[] slotPort1Parts = slotPort1.Split('+');

                            if (slotPort1Parts.Length == 2)
                            {
                                row["SLOT"] = slotPort1Parts[0];
                                row["PORT"] = parts[1];

                                // Create a new row (newRow) and copy the changes
                                var newRow = new Dictionary<string, string>(row);
                                newRow["SLOT"] = slotPort1Parts[1]; // Update SLOT in the new row
                                newRow["PORT"] = parts[1]; // Update PORT in the new row

                                int currentIndex = data.IndexOf(row); // Get the index of the current row
                                data.Insert(currentIndex + 1, newRow); // Insert the new row right after the current row
                            }
                        }
                    }
                    else
                    {
                        string[] slotPortParts = linkValue.Split('/');

                        if (slotPortParts.Length == 2)
                        {
                            row["SLOT"] = slotPortParts[0];
                            row["PORT"] = slotPortParts[1];
                        }
                    }
                }
            }
        }



        private void ModifyDataForRFInputPower(List<Dictionary<string, string>> data, string fileName)
        {
            List<Dictionary<string, string>> rowsToDelete = new List<Dictionary<string, string>>();
            // Apply modifications specific to files containing "RFInputPower" in the name
            foreach (var row in data)
            {
                if (row.ContainsKey("FarEndTID") && row["FarEndTID"] == "----")
                {
                    rowsToDelete.Add(row);

                }
                // Add or modify other columns as needed
                // row["new_column_rfinput"] = "new_value_for_rfinput"; // Add a new column
                row.Remove("Position"); // Delete a specific column
                row.Remove("MeanRxLevel1m");
                row.Remove("IdLogNum");
                row.Remove("FailureDescription");

            }

            foreach (var rowToDelete in rowsToDelete)
            {
                data.Remove(rowToDelete);
            }

            AddNetworkSidAndDatetimeKeyColumns(data, fileName);
            AddSlotAndPortColumnsTwo(data);

        }


        private void AddSlotAndPortColumnsTwo(List<Dictionary<string, string>> data)
        {
            foreach (var row in data.ToList()) // Create a copy of the list to avoid concurrent modification
            {
                if (row.ContainsKey("Object"))
                {
                    string linkValue = row["Object"];
                    string[] parts = linkValue.Split('.');

                    string slotPort1 = parts[0];
                    row["SLOT"] = slotPort1 + "+";

                    string[] slotPort2 = parts[1].Split('/');
                    row["PORT"] = slotPort2[0];
                }
            }
        }

        private List<Dictionary<string, string>> ReadCsvFile(string filePath)
        {
            List<Dictionary<string, string>> data = new List<Dictionary<string, string>>();
            using (var reader = new StreamReader(filePath))
            {
                string[] headers = reader.ReadLine().Split(',');
                while (!reader.EndOfStream)
                {
                    string[] values = reader.ReadLine().Split(',');
                    var rowData = new Dictionary<string, string>();
                    for (int i = 0; i < headers.Length; i++)
                    {
                        rowData[headers[i]] = values[i];
                    }
                    data.Add(rowData);
                }
            }
            return data;
        }

        private void WriteCsvFile(string filePath, List<Dictionary<string, string>> data)
        {
            using (var writer = new StreamWriter(filePath))
            {
                if (data.Count > 0)
                {
                    // Write headers
                    writer.WriteLine(string.Join(",", data[0].Keys));
                }

                foreach (var row in data)
                {
                    writer.WriteLine(string.Join(",", row.Values));
                }
            }
        }


    }
}