using System.Threading.Channels;
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
            // Good luck!
        }
    }
}