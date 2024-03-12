using System.IO;
using System.Reflection;
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

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();

        await foreach (FileSystemInfo item in channelReader.ReadAllAsync(cancellationToken))
        {
            if (item is DirectoryInfo directory)
            {
                await IndexDirectoryAsync(directory, connection);
            }
            else if (item is FileInfo file)
            {
                await IndexFileAsync(file, connection);
            }
        }

        await connection.CloseAsync();
    }

    private async Task IndexDirectoryAsync(DirectoryInfo directoryInfo, SqlConnection connection)
    {
        var commandText = "IF NOT EXISTS (SE LECT 1 FROM Directories WHERE Path = @Path) " +
                       "   INSERT INTO Directories (Path, Name, DirectoryPath, LastModified) " +
                       "   VALUES (@Path, @Name, @DirectoryPath, @LastModified) " +
                       "ELSE IF (SELECT LastModified FROM Directories WHERE Path = @Path) <> @LastModified " +
                       "   UPDATE Directories SET Name = @Name, DirectoryPath = @DirectoryPath, LastModified = @LastModified " +
                       "   WHERE Path = @Path";

        await using var command = new SqlCommand(commandText, connection);
        command.Parameters.AddWithValue("@Path", directoryInfo.FullName);
        command.Parameters.AddWithValue("@Name", directoryInfo.Name);
        command.Parameters.AddWithValue("@DirectoryPath", directoryInfo.Parent?.FullName ?? string.Empty);
        command.Parameters.AddWithValue("@LastModified", directoryInfo.LastWriteTimeUtc);

        await command.ExecuteNonQueryAsync();
    }

    private async Task IndexFileAsync(FileInfo fileInfo, SqlConnection connection)
    {
        var commandText = "IF NOT EXISTS (SELECT 1 FROM Files WHERE Path = @Path) " +
                      "   INSERT INTO Files (Path, Name, DirectoryPath, Size, LastModified) " +
                      "   VALUES (@Path, @Name, @DirectoryPath, @Size, @LastModified) " +
                      "ELSE IF (SELECT LastModified FROM Files WHERE Path = @Path) <> @LastModified " +
                      "   UPDATE Files SET Name = @Name, DirectoryPath = @DirectoryPath, Size = @Size, LastModified = @LastModified " +
                      "   WHERE Path = @Path";

        await using var command = new SqlCommand(commandText, connection);
        command.Parameters.AddWithValue("@Path", fileInfo.FullName);
        command.Parameters.AddWithValue("@Name", fileInfo.Name);
        command.Parameters.AddWithValue("@DirectoryPath", fileInfo.Directory?.FullName ?? string.Empty);
        command.Parameters.AddWithValue("@Size", fileInfo.Length);
        command.Parameters.AddWithValue("@LastModified", fileInfo.LastWriteTimeUtc);

        await command.ExecuteNonQueryAsync();

    }
}

