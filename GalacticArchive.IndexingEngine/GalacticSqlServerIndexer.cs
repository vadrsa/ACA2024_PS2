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

        using (SqlConnection connection = new SqlConnection(connectionString))
        {
            await connection.OpenAsync();
            string selectQuery = "SELECT TOP 1 * FROM Directories WHERE [Path] = @Path";
            using (SqlCommand selectCommand = new SqlCommand(selectQuery, connection))
            {
                selectCommand.Parameters.AddWithValue("@Path", path);
                bool directoryExists = (await selectCommand.ExecuteScalarAsync() != null);

                if (!directoryExists)
                {
                    string insertQuery = "INSERT INTO Directories ([Path], [Name], [DirectoryPath], [LastModified]) VALUES (@Path, @Name, @DirectoryPath, @LastModified)";
                    using (SqlCommand insertCommand = new SqlCommand(insertQuery, connection))
                    {
                        insertCommand.Parameters.AddWithValue("@Path", path);
                        insertCommand.Parameters.AddWithValue("@Name", name);
                        insertCommand.Parameters.AddWithValue("@DirectoryPath", directoryPath);
                        insertCommand.Parameters.AddWithValue("@LastModified", lastModified);
                        await insertCommand.ExecuteNonQueryAsync();
                    }
                }
                else
                {
                    string updateQuery = "UPDATE Directories SET [Name] = @Name, [DirectoryPath] = @DirectoryPath, [LastModified] = @LastModified WHERE [Path] = @Path " +
                                         "AND ([Name] <> @Name OR [DirectoryPath] <> @DirectoryPath)";
                    using (SqlCommand updateCommand = new SqlCommand(updateQuery, connection))
                    {
                        updateCommand.Parameters.AddWithValue("@Name", name);
                        updateCommand.Parameters.AddWithValue("@DirectoryPath", directoryPath);
                        updateCommand.Parameters.AddWithValue("@LastModified", lastModified);
                        updateCommand.Parameters.AddWithValue("@Path", path);
                        await updateCommand.ExecuteNonQueryAsync();
                    }
                }
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

        using (SqlConnection connection = new SqlConnection(connectionString))
        {
            await connection.OpenAsync();
            string selectQuery = "SELECT TOP 1 * FROM Files WHERE [Path] = @Path";
            using (SqlCommand selectCommand = new SqlCommand(selectQuery, connection))
            {
                selectCommand.Parameters.AddWithValue("@Path", path);
                bool fileExists = (await selectCommand.ExecuteScalarAsync() != null);
                if (!fileExists)
                {
                    string insertQuery = "INSERT INTO Files ([Path], [Name], [DirectoryPath], [Size], [LastModified]) VALUES (@Path, @Name, @DirectoryPath, @Size, @LastModified)";
                    using (SqlCommand insertCommand = new SqlCommand(insertQuery, connection))
                    {
                        insertCommand.Parameters.AddWithValue("@Path", path);
                        insertCommand.Parameters.AddWithValue("@Name", name);
                        insertCommand.Parameters.AddWithValue("@DirectoryPath", directoryPath);
                        insertCommand.Parameters.AddWithValue("@Size", size);
                        insertCommand.Parameters.AddWithValue("@LastModified", lastModified);
                        await insertCommand.ExecuteNonQueryAsync();
                    }
                }
                else
                {
                    string updateQuery = "UPDATE Files SET [Name] = @Name, [DirectoryPath] = @DirectoryPath, [Size] = @Size, [LastModified] = @LastModified WHERE [Path] = @Path " +
                                         "AND ([Name] <> @Name OR [DirectoryPath] <> @DirectoryPath OR [Size] <> @Size)";
                    using (SqlCommand updateCommand = new SqlCommand(updateQuery, connection))
                    {
                        updateCommand.Parameters.AddWithValue("@Path", path);
                        updateCommand.Parameters.AddWithValue("@Name", name);
                        updateCommand.Parameters.AddWithValue("@DirectoryPath", directoryPath);
                        updateCommand.Parameters.AddWithValue("@Size", size);
                        updateCommand.Parameters.AddWithValue("@LastModified", lastModified);
                        await updateCommand.ExecuteNonQueryAsync();
                    }
                }
            }
        }
    }
}