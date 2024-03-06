using GalacticArchive.IndexingEngine;
using System.Threading.Channels;

namespace GalacticArchive
{
    public class GalacticWorker : BackgroundService
    {
        private readonly ILogger<GalacticWorker> _logger;
        private readonly IHostApplicationLifetime _lifetime;
        private readonly GalacticSqlServerIndexer _sqlServerIndexer;
        private readonly IConfiguration _configuration;

        public GalacticWorker(IConfiguration configuration, IHostApplicationLifetime lifetime, GalacticSqlServerIndexer sqlServerIndexer, ILogger<GalacticWorker> logger)
        {
            _logger = logger;
            _lifetime = lifetime;
            _sqlServerIndexer = sqlServerIndexer;
            _configuration = configuration;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            // wait for host to start
            while (!_lifetime.ApplicationStarted.IsCancellationRequested)
            {
                await Task.Delay(1000, stoppingToken);
            }

            // allow a maximum of 100 items to be in queue at a given point in time
            // if the consumer(Indexing Service) is processing slowly, we(the producer) will stop producing after a backpressure of 100 items
            var channel = Channel.CreateBounded<FileSystemInfo>(new BoundedChannelOptions(100));
            try
            {
                stoppingToken.ThrowIfCancellationRequested();

                var rootPath = _configuration.GetValue<string>("RootFolder");
                if (rootPath == null || !Directory.Exists(rootPath))
                {
                    throw new InvalidOperationException("The configuration variable 'RootFolder' is either not specified or the folder doesn't exist. Please check the 'appsettings' file and set a correct value");
                }
                var connectionString = _configuration.GetValue<string>("ConnectionStrings:DefaultConnection");
                if (connectionString == null)
                {
                    throw new InvalidOperationException("The configuration variable 'ConnectionStrings:DefaultConnection' is not specified. Please check the 'appsettings' file and set a correct value");
                }

                // await MigrationRunner.RunAsync(connectionString);

                // We are intentionally not awaiting the returned task
                // the idea is to let the reader/consumer task run in parallel with the current method.
                // Stopping the task will be done through the stoppingToken and completing the channel
                // https://learn.microsoft.com/en-us/dotnet/api/system.threading.cancellationtoken
                Task consumerTask = _sqlServerIndexer.RunAsync(channel.Reader, stoppingToken);
                Task producerTask = ProduceAsync(rootPath, channel.Writer, stoppingToken);

                // now both consumerTask and producerTask are running in parallel, wait until one of them finishes
                var finishedTask = await Task.WhenAny(producerTask, consumerTask);
                // normal case
                if (finishedTask == producerTask)
                {
                    // complete the writer
                    channel.Writer.Complete();

                    // when the producer task is finished, we should wait for the consumer to finish consuming to stop.
                    await consumerTask;

                    // await producer task so that it throws it's exception, if any, and we will stop the application
                    await producerTask;
                }
                // in case consumer finishes earlier then producer(error case)
                else if (finishedTask == consumerTask)
                {
                    // await consumer task so that it throws it's exception and we will stop the application
                    await consumerTask;
                }

                _logger.LogInformation("Galactic archive data indexer finished its mission. Congratulations!");
            }
            catch (OperationCanceledException)
            {
                _logger.LogWarning("Galactic archive data collection job is cancelled prematurely");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Galactic archive data collection job failed to finish its mission");
            }
            finally
            {
                // stop the application on finish
                _lifetime.StopApplication();
            }
        }

        private async Task ProduceAsync(string rootPath, ChannelWriter<FileSystemInfo> writer, CancellationToken cancellationToken)
        {
            var directoryQueue = new Queue<string>();

            directoryQueue.Enqueue(rootPath);
            var directoryCount = 0;
            var fileCount = 0;
            while (directoryQueue.TryDequeue(out var directory))
            {
                cancellationToken.ThrowIfCancellationRequested();

                try
                {
                    foreach (var subDirectory in Directory.EnumerateDirectories(directory))
                    {
                        await writer.WriteAsync(new DirectoryInfo(subDirectory), cancellationToken);
                        directoryCount++;
                        directoryQueue.Enqueue(subDirectory);
                    }
                }
                // catch, log and continue
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Galactic archive data collection job failed while queueing subdirectoris of '{Directory}'", directory);
                }

                try
                {
                    foreach (var file in Directory.EnumerateFiles(directory))
                    {
                        await writer.WriteAsync(new FileInfo(file), cancellationToken);
                        fileCount++;
                    }
                }
                // catch, log and continue
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Galactic archive data collection job failed while queueing files of '{Directory}'", directory);
                }
            }

            _logger.LogInformation("Galactic archive data collection job finished its mission. Found {DirectoryCount} directories and {fileCount} files", directoryCount, fileCount);
        }
    }
}