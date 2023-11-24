using System;
using System.IO;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace BabyNiProject
{
    class Program
    {
        static void Main()
        {
            // Setup configuration
            IConfiguration configuration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json")
                .Build();

            // Setup dependency injection
            var serviceProvider = new ServiceCollection()
                .AddSingleton<IConfiguration>(configuration)
                .AddTransient<FileLoader>()
                .BuildServiceProvider();

            var fileLoader = serviceProvider.GetRequiredService<FileLoader>();

            string directoryToMonitor = GetDirectoryToMonitor();

            if (DirectoryExists(directoryToMonitor))
            {
                string parserDirectory = Path.Combine(directoryToMonitor, "parser");
                string archiveDirectory = Path.Combine(directoryToMonitor, "archive");

                using (var monitor = new FileMonitor(directoryToMonitor, parserDirectory, archiveDirectory,fileLoader, configuration))
                {
                    monitor.Start();

                    Console.WriteLine("File monitoring started. Press Enter to exit.");
                    monitor.ProcessExistingFiles(); // Process existing .txt files on startup

                    Console.ReadLine();

                }
            }
            else
            {
                Console.WriteLine("The specified directory does not exist.");
                Console.WriteLine("Press Enter to exit.");
                Console.ReadLine();
            }
        }

        static string GetDirectoryToMonitor()
        {
            return @"C:\Users\User\Desktop\Project\BabyNiProject";
        }

        static bool DirectoryExists(string path)
        {
            return Directory.Exists(path);
        }
    }
}