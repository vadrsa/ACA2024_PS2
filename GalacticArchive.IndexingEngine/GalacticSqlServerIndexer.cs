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
        
        // Channels are an advanced topic, but if interested, you can learn about them here
        // https://devblogs.microsoft.com/dotnet/an-introduction-to-system-threading-channels/
        await foreach (FileSystemInfo item in channelReader.ReadAllAsync(cancellationToken))
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                if (item is DirectoryInfo directoryInfo)
                {
                    await WriteDirectoryInDBAsync(connection, directoryInfo);
                }
                else if (item is FileInfo fileInfo)
                {
                    await WriteFileInDbAsync(connection, fileInfo);
                }
            }
            // Good luck!
        }
    }
    public async Task WriteFileInDbAsync(SqlConnection connection, FileInfo info) { 
            string sqlcommand = @"MERGE INTO Files AS Target
                                USING (VALUES(@Path,@DirectoryPath,@Name,@ModifiedTime,@Size)) AS Source (Path,DirectoryPath,Name, LastModified,Size)
                                ON target.Path = source.Path
                                WHEN MATCHED THEN
                                UPDATE SET target.Name = source.Name, target.Size = source.Size, target.LastModified = source.LastModified
                                WHEN NOT MATCHED BY TARGET THEN
                                INSERT (Path, DirectoryPath,Name, Size, LastModified) VALUES (source.Path,source.DirectoryPath, source.Name, source.Size, source.LastModified);
                                ";
            using(SqlCommand command=new SqlCommand(sqlcommand,connection))
            {
                command.Parameters.AddWithValue("@Path",info.FullName);
                command.Parameters.AddWithValue("@DirectoryPath", info.DirectoryName);
                command.Parameters.AddWithValue("@Name", info.Name);
                command.Parameters.AddWithValue("@ModifiedTime", info.LastWriteTime);
                command.Parameters.AddWithValue("@Size", info.Length);
                command.ExecuteNonQuery();
            }
    }
    public async Task WriteDirectoryInDBAsync(SqlConnection connection, DirectoryInfo info)
    {
        string sqlcommand = @"MERGE INTO Directories AS Target
                                USING (VALUES(@Path,@DirectoryPath,@Name,@ModifiedTime)) AS Source (Path,DirectoryPath,Name, LastModified)
                                ON target.Path = source.Path
                                WHEN MATCHED THEN
                                UPDATE SET target.Name = source.Name, target.LastModified = source.LastModified
                                WHEN NOT MATCHED BY TARGET THEN
                                INSERT (Path, DirectoryPath,Name, LastModified) VALUES (source.Path,source.DirectoryPath, source.Name, source.LastModified);
                                ";
        using (SqlCommand command = new SqlCommand(sqlcommand, connection))
        {
            command.Parameters.AddWithValue("@Path", info.FullName);
            command.Parameters.AddWithValue("@DirectoryPath", info.Parent?.FullName);
            command.Parameters.AddWithValue("@Name", info.Name);
            command.Parameters.AddWithValue("@ModifiedTime", info.LastWriteTime);
            command.ExecuteNonQuery();
        }
    }
}