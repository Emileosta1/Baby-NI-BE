using System;
using System.Configuration;
using System.IO;

namespace BabyNiProject
{
    public class FileMonitor : IDisposable
    {
        private readonly IConfiguration configuration;
        private readonly FileSystemWatcher watcher;
        private readonly string fileFilter;
        private readonly string parserDirectory;
        private readonly string archiveDirectory;
        private readonly FileParser fileParser = new FileParser();
        private readonly FileLoader fileLoader;
        private readonly Aggregator aggregator;

        public FileMonitor(string directoryPath, string parserDir, string archiveDir, FileLoader loader, IConfiguration configuration)
        {
            watcher = new FileSystemWatcher
            {
                Path = directoryPath,
                NotifyFilter = NotifyFilters.LastWrite | NotifyFilters.FileName | NotifyFilters.DirectoryName
            };

            fileFilter = "*.txt";
            parserDirectory = parserDir;
            archiveDirectory = archiveDir;

            this.configuration = configuration;
            watcher.Changed += OnFileChanged;
            watcher.Created += OnFileCreated;
            watcher.Deleted += OnFileDeleted;
            watcher.Renamed += OnFileRenamed;
            fileLoader = loader;
            aggregator = new Aggregator(configuration);

            // Check if parser directory exists, if not, create it
            if (!Directory.Exists(parserDirectory))
            {
                Directory.CreateDirectory(parserDirectory);
            }
        }

        public void Start()
        {
            try
            {
                watcher.EnableRaisingEvents = true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error while starting file monitoring: {ex.Message}");
            }
        }

        public void Stop()
        {
            try
            {
                watcher.EnableRaisingEvents = false;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error while stopping file monitoring: {ex.Message}");
            }
        }

        public void Dispose()
        {
            watcher.Dispose();
        }

        public void ProcessExistingFiles()
        {
            foreach (var filePath in Directory.GetFiles(watcher.Path, fileFilter))
            {
                string fileName = Path.GetFileName(filePath);
                string parserDestinationPath = Path.Combine(parserDirectory, fileName);
                string archiveDestinationPath = Path.Combine(archiveDirectory, fileName);

                if (!File.Exists(archiveDestinationPath))
                {
                    File.Move(filePath, parserDestinationPath); // Move to the 'parser' folder
                    Console.WriteLine($"Moved {fileName} to the 'parser' folder.");

                    File.Copy(parserDestinationPath, archiveDestinationPath); // Copy to the 'archive' folder
                    Console.WriteLine($"Copied {fileName} to the 'archive' folder.");

                    fileParser.ConvertToCsv(parserDestinationPath); // Convert the extension to .csv
                    Console.WriteLine($"Converted {fileName} to CSV.");
                    fileLoader.LoadFilesToVertica(parserDirectory);

                    aggregator.AggregateData();
                }
                else
                {
                    Console.WriteLine("Duplicate File Detected and Deleted!");
                    File.Delete(filePath);
                }
            }
        }

        private void OnFileChanged(object sender, FileSystemEventArgs e)
        {
            try
            {
                if (IsFileExtensionValid(e.Name, ".txt"))
                {
                    string filePath = Path.Combine(parserDirectory, e.Name);

                    if (File.Exists(filePath))
                    {
                        // Convert the file to CSV when changed in the "parser" folder
                        fileParser.ConvertToCsv(filePath);
                        Console.WriteLine($"Converted {e.Name} to CSV.");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error handling file change event: {ex.Message}");
            }
        }

        private void OnFileCreated(object sender, FileSystemEventArgs e)
        {
            try
            {
                if (IsFileExtensionValid(e.Name, ".txt"))
                {
                    string sourceFilePath = Path.Combine(watcher.Path, e.Name);
                    string fileName = Path.GetFileName(sourceFilePath);

                    string parserDestinationPath = Path.Combine(parserDirectory, fileName);
                    string archiveDestinationPath = Path.Combine(archiveDirectory, fileName);

                    if (!File.Exists(archiveDestinationPath))
                    {
                        File.Copy(sourceFilePath, parserDestinationPath);
                        Console.WriteLine($"Copied {fileName} to the 'parser' folder.");

                        fileParser.ConvertToCsv(parserDestinationPath);

                        File.Copy(sourceFilePath, archiveDestinationPath);
                        Console.WriteLine($"Copied {fileName} to the 'archive' folder.");

                        File.Delete(sourceFilePath);
                        Console.WriteLine($"Deleted {fileName} from the original folder.");
                        fileLoader.LoadFilesToVertica(parserDirectory);
                        aggregator.AggregateData();
                    }
                    else
                    {
                        File.Delete(sourceFilePath);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error handling file creation event: {ex.Message}");
            }
        }

        private void OnFileDeleted(object sender, FileSystemEventArgs e)
        {
            try
            {
                if (IsFileExtensionValid(e.Name, ".txt"))
                {
                    //Console.WriteLine($"File {e.Name} has been deleted.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error handling file deletion event: {ex.Message}");
            }
        }

        private void OnFileRenamed(object sender, RenamedEventArgs e)
        {
            try
            {
                if (IsFileExtensionValid(e.Name, ".txt") || IsFileExtensionValid(e.OldName, ".txt"))
                {
                    Console.WriteLine($"File {e.OldName} has been renamed to {e.Name}.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error handling file rename event: {ex.Message}");
            }
        }

        private bool IsFileExtensionValid(string fileName, string extension)
        {
            return string.Equals(Path.GetExtension(fileName), extension, StringComparison.OrdinalIgnoreCase);
        }
    }
}
