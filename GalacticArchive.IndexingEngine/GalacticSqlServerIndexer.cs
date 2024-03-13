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
        var connectionString = _options.ConnectionString;

        // Channels are an advanced topic, but if interested, you can learn about them here
        // https://devblogs.microsoft.com/dotnet/an-introduction-to-system-threading-channels/
        await foreach (FileSystemInfo item in channelReader.ReadAllAsync(cancellationToken))
        {
            try
            {
                if (item is DirectoryInfo directory)
                {
                    await using var connection = new SqlConnection(connectionString);
                    await connection.OpenAsync(cancellationToken);

                    var directoryExists = await DirectoryExistsInDatabaseAsync(directory.FullName, connection);

                    if (!directoryExists)
                    {
                        await InsertDirectoryAsync(directory, connection);
                    }
                    else
                    {
                        await UpdateDirectoryLastModifiedAsync(directory, connection);
                    }
                }
                else if (item is FileInfo file)
                {
                    await using var connection = new SqlConnection(connectionString);
                    await connection.OpenAsync(cancellationToken);

                    var fileExists = await FileExistsInDatabaseAsync(file.FullName, connection);

                    if (!fileExists)
                    {
                        await InsertFileAsync(file, connection);
                    }
                    else
                    {
                        await UpdateFileLastModifiedAndSizeAsync(file, connection);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error processing file system item: {ex.Message}");
            }
        }
    }

    private async Task<bool> DirectoryExistsInDatabaseAsync(string directoryPath, SqlConnection connection)
    {
        var sql = @"SELECT COUNT(*) FROM dbo.Directories WHERE Path = @directoryPath";
        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@directoryPath", directoryPath);
        return (int)await command.ExecuteScalarAsync() > 0;
    }

    private async Task InsertDirectoryAsync(DirectoryInfo directory, SqlConnection connection)
    {
        var sql = @"INSERT INTO dbo.Directories (Path, LastModified) VALUES (@path, @lastModified)";
        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@path", directory.FullName);
        command.Parameters.AddWithValue("@lastModified", directory.LastWriteTimeUtc);
        await command.ExecuteNonQueryAsync();
    }

    private async Task UpdateDirectoryLastModifiedAsync(DirectoryInfo directory, SqlConnection connection)
    {
        var sql = @"UPDATE dbo.Directories SET LastModified = @lastModified WHERE Path = @path";
        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@path", directory.FullName);
        command.Parameters.AddWithValue("@lastModified", directory.LastWriteTimeUtc);
        await command.ExecuteNonQueryAsync();
    }

    private async Task<bool> FileExistsInDatabaseAsync(string filePath, SqlConnection connection)
    {
        var sql = @"SELECT COUNT(*) FROM dbo.Files WHERE Path = @filePath";
        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@filePath", filePath);
        return (int)await command.ExecuteScalarAsync() > 0;
    }

    private async Task InsertFileAsync(FileInfo file, SqlConnection connection)
    {
        var sql = @"INSERT INTO dbo.Files (Path, Size, LastModified) VALUES (@path, @size, @lastModified)";
        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@path", file.FullName);
        command.Parameters.AddWithValue("@size", file.Length);
        command.Parameters.AddWithValue("@lastModified", file.LastWriteTimeUtc);
        await command.ExecuteNonQueryAsync();
    }

    private async Task UpdateFileLastModifiedAndSizeAsync(FileInfo file, SqlConnection connection)
    {
        var sql = @"UPDATE dbo.Files SET Size = @size, LastModified = @lastModified WHERE Path = @path";
        await using var command = new SqlCommand(sql, connection);
        command.Parameters.AddWithValue("@path", file.FullName);
        command.Parameters.AddWithValue("@size", file.Length);
        command.Parameters.AddWithValue("@lastModified", file.LastWriteTimeUtc);
        await command.ExecuteNonQueryAsync();
    }
}