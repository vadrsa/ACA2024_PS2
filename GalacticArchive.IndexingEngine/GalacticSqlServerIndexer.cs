using System.Data;
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

        List<Task> tasks = new List<Task>();
        SemaphoreSlim tasksSemaphore = new SemaphoreSlim(4, 4);
        
        // Channels are an advanced topic, but if interested, you can learn about them here
        // https://devblogs.microsoft.com/dotnet/an-introduction-to-system-threading-channels/
        await foreach (FileSystemInfo item in channelReader.ReadAllAsync(cancellationToken))
        {
            await tasksSemaphore.WaitAsync(cancellationToken);
            Task? task = null;
            switch (item)
            {

                case DirectoryInfo directoryInfo:
                    task = SyncDirectoryAsync(directoryInfo, connectionString, cancellationToken).ContinueWith(
                        _ =>
                        {
                            tasksSemaphore.Release();
                        }, cancellationToken);

                    break;
                case FileInfo fileInfo:
                    task = SyncFileAsync(fileInfo, connectionString, cancellationToken).ContinueWith(
                        _ =>
                        {
                            tasksSemaphore.Release();
                        }, cancellationToken);
                    
                    break;
                
            }

            if (task != null)
            {
                tasks.Add(task);
            }
        }
        
        await Task.WhenAll(tasks);
    }

    private async Task SyncFileAsync(FileInfo fileInfo, string connectionString, CancellationToken cancellationToken)
    {
        var (exists, existingId, existingSize) = await GetFileIdIfExistsAsync(fileInfo, connectionString, cancellationToken);
        var utcNow = DateTime.UtcNow;
        await using var connection = new SqlConnection(connectionString);
        
        if (!exists)
        {
            await using var command = new SqlCommand("INSERT INTO Files (Path, Name, DirectoryPath, Size, LastModified) VALUES (@Path, @Name, @DirectoryPath, @Size, @LastModified)", connection);
            command.Parameters.AddWithValue("@Path", fileInfo.FullName);
            command.Parameters.AddWithValue("@Name", fileInfo.Name);
            command.Parameters.AddWithValue("@DirectoryPath", fileInfo.Directory?.FullName ?? "");
            command.Parameters.AddWithValue("@Size", fileInfo.Length);
            command.Parameters.AddWithValue("@LastModified", utcNow);
            
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
        else if(existingSize != fileInfo.Length)
        {
            await using var command = new SqlCommand("UPDATE Files SET Size=@Size, LastModified=@LastModified WHERE Id=@Id", connection);
            command.Parameters.AddWithValue("@Size", fileInfo.Length);
            command.Parameters.AddWithValue("@LastModified", utcNow);
            command.Parameters.AddWithValue("@Id", existingId);
            
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    private async Task<(bool Exists, int Id, long Size)> GetFileIdIfExistsAsync(FileInfo fileInfo, string connectionString, CancellationToken cancellationToken)
    {
        await using SqlConnection connection = new SqlConnection(connectionString);
        
        await using SqlCommand selectCommand = new SqlCommand("SELECT Id, Size FROM Files WHERE Path=@Path", connection);
        selectCommand.Parameters.AddWithValue("Path", fileInfo.FullName);
        await using var selectReader = await selectCommand.ExecuteReaderAsync(cancellationToken);

        if (!await selectReader.ReadAsync(cancellationToken))
        {
            return (false, 0, 0);
        }

        return (true, selectReader.GetInt32("Id"), selectReader.GetInt64("Size"));
    }

    private Task SyncDirectoryAsync(DirectoryInfo directoryInfo, string connectionString,  CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}