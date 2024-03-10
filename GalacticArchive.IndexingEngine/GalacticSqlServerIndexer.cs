using System.Threading.Channels;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace GalacticArchive.IndexingEngine;

public class GalacticSqlServerIndexer
{
    private readonly ILogger<GalacticSqlServerIndexer> _logger;
    private readonly GalacticOptions _options;

    public GalacticSqlServerIndexer(ILogger<GalacticSqlServerIndexer> logger, IOptions<GalacticOptions> options)
    {
        _logger = logger;
        _options = options.Value;
    }

    public async Task RunAsync(ChannelReader<FileSystemInfo> channelReader, CancellationToken cancellationToken)
    {
        string connectionString = _options.ConnectionString;

        try
        {
            await foreach (FileSystemInfo item in channelReader.ReadAllAsync(cancellationToken))
            {
                await ProcessFileSystemInfoAsync(item, connectionString, cancellationToken);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error occurred while indexing file system information.");
        }
    }

    private async Task ProcessFileSystemInfoAsync(FileSystemInfo info, string connectionString, CancellationToken cancellationToken)
    {
        if (info is DirectoryInfo directory)
        {
            await ProcessDirectoryAsync(directory, connectionString, cancellationToken);
        }
        else if (info is FileInfo file)
        {
            await ProcessFileAsync(file, connectionString, cancellationToken);
        }
    }

    private async Task ProcessDirectoryAsync(DirectoryInfo directory, string connectionString, CancellationToken cancellationToken)
    {
        _logger.LogInformation($"Processing directory: {directory.Name}");

        string path = directory.FullName;
        string name = directory.Name;
        string directoryPath = directory.FullName;
        DateTime lastModified = DateTime.UtcNow;

        string query = "IF NOT EXISTS (SELECT TOP 1 FROM Directories WHERE [Path] = @Path) " +
                        "BEGIN " +
                        "INSERT INTO Directories ([Path], [Name], [DirectoryPath], [LastModified]) " +
                        "VALUES (@Path, @Name, @DirectoryPath, @LastModified) " +
                        "END " +
                        "ELSE " +
                        "BEGIN " +
                        "UPDATE Directories SET [Name] = @Name, [DirectoryPath] = @DirectoryPath, [LastModified] = @LastModified " +
                        "WHERE [Path] = @Path " +
                        "END ";
        using (SqlConnection connection = new SqlConnection(connectionString))
        {
            using (SqlCommand command = new SqlCommand(query, connection))
            {
                command.Parameters.AddWithValue("@Path", path);
                command.Parameters.AddWithValue("@Name", name);
                command.Parameters.AddWithValue("@DirectoryPath", directoryPath);
                command.Parameters.AddWithValue("@LastModified", lastModified);

                await command.ExecuteNonQueryAsync();
            }
        }
    }

    private async Task ProcessFileAsync(FileInfo file, string connectionString, CancellationToken cancellationToken)
    {
        _logger.LogInformation($"Processing file: {file.Name}");
        string path = file.FullName;
        string name = file.Name;
        string directoryPath = file.FullName;
        long size = file.Length;
        DateTime lastModified = DateTime.UtcNow;

        string query = "IF NOT EXISTS (SELECT TOP 1 FROM Files WHERE [Path] = @Path) " +
                        "BEGIN " +
                        "INSERT INTO Files ([Path], [Name], [DirectoryPath], [Size], [LastModified]) " +
                        "VALUES (@Path, @Name, @DirectoryPath, @Size, @LastModified) " +
                        "END " +
                        "ELSE " +
                        "BEGIN " +
                        "UPDATE Files SET [Name] = @Name, [DirectoryPath] = @DirectoryPath, [Size] = @Size, [LastModified] = @LastModified " +
                        "WHERE [Path] = @Path " +
                        "END ";
        using (SqlConnection connection = new SqlConnection(connectionString))
        {
            using (SqlCommand command = new SqlCommand(query, connection))
            {
                command.Parameters.AddWithValue("@Path", path);
                command.Parameters.AddWithValue("@Name", name);
                command.Parameters.AddWithValue("@DirectoryPath", directoryPath);
                command.Parameters.AddWithValue("@Size", size);
                command.Parameters.AddWithValue("@LastModified", lastModified);

                await command.ExecuteNonQueryAsync();
            }
        }
    }
}