using System.Data;
using System.Threading.Channels;
using GalacticArchive.IndexingEngine.Tests.Entities;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.IdentityModel.Tokens;
using Xunit.Abstractions;
using Testcontainers.MsSql;

namespace GalacticArchive.IndexingEngine.Tests;

public class GalacticIndexerTest : IAsyncLifetime
{
    private readonly MsSqlContainer _sqlServerContainer;

    public GalacticIndexerTest(ITestOutputHelper testOutputHelper)
    {
        TestOutputHelper = testOutputHelper;
        _sqlServerContainer = new MsSqlBuilder()
            .Build();
    }

    private ITestOutputHelper TestOutputHelper { get; }
    private string ServerConnectionString => _sqlServerContainer.GetConnectionString();

    [Fact]
    public async Task NoItems_Run_TablesAreEmpty()
    {
        // setup
        await using var disposableDatabase = await SetupNewDbAsync();
        var indexer = GetIndexer(disposableDatabase);

        var channel = Channel.CreateUnbounded<FileSystemInfo>();
        // act
        var readTask = indexer.RunAsync(channel.Reader, CancellationToken.None);
        channel.Writer.Complete();
        await readTask;
        // verify
        var files = await GetAllFilesAsync(disposableDatabase.DbConnectionString);
        var directories = await GetAllDirectoriesAsync(disposableDatabase.DbConnectionString);

        files.Should().BeEmpty();
        directories.Should().BeEmpty();
    }
    
    [Fact]
    public async Task NotEmpty_Run_TablesAreCorrect()
    {
        // setup
        await using var disposableDatabase = await SetupNewDbAsync();
        var indexer = GetIndexer(disposableDatabase);
        
        var channel = Channel.CreateUnbounded<FileSystemInfo>();
        
        using var rootPath = new DisposableDirectory();
        var file1 = rootPath.CreateFile();
        var directory1 = rootPath.CreateDirectory();
        var directory1File2 = directory1.CreateFile();
        
        // act
        var startTime = DateTime.UtcNow;
        var readTask = indexer.RunAsync(channel.Reader, CancellationToken.None);

        channel.Writer.TryWrite(file1);
        channel.Writer.TryWrite(directory1File2);
        channel.Writer.TryWrite(directory1.Info);
        channel.Writer.Complete();
        await readTask;
        
        // verify
        var files = await GetAllFilesAsync(disposableDatabase.DbConnectionString);
        var directories = await GetAllDirectoriesAsync(disposableDatabase.DbConnectionString);

        files.Should().HaveCount(2);
        var file1Assert = files.Should().ContainKey(NormalizePath(file1.FullName))
            .WhoseValue;
        
        file1Assert.Name
            .Should()
            .Be(file1.Name);
        file1Assert.Path
            .Should()
            .Be(NormalizePath(file1.FullName));
        file1Assert.ParentPath
            .Should()
            .Be(NormalizePath(rootPath.Value));
        file1Assert.Size
            .Should()
            .Be(file1.Length);
        file1Assert.LastModified
            .Should()
            .BeAfter(startTime);
        
        var file2Assert = files.Should().ContainKey(NormalizePath(directory1File2.FullName))
            .WhoseValue;
        file2Assert.Name
            .Should()
            .Be(directory1File2.Name);
        file2Assert.Path
            .Should()
            .Be(NormalizePath(directory1File2.FullName));
        file2Assert.ParentPath
            .Should()
            .Be(NormalizePath(directory1.Value));
        file2Assert.Size
            .Should()
            .Be(directory1File2.Length);
        file2Assert.LastModified
            .Should()
            .BeAfter(startTime);

        directories.Should().HaveCount(1);
        var directory1Assert = directories.Should().ContainKey(NormalizePath(directory1.Value))
            .WhoseValue;

        directory1Assert.Name
            .Should()
            .Be(directory1.Info.Name);
        directory1Assert.Path
            .Should()
            .Be(NormalizePath(directory1.Value));
        directory1Assert.ParentPath
            .Should()
            .Be(NormalizePath(rootPath.Value));
        directory1Assert.LastModified
            .Should()
            .BeAfter(startTime);
    }

    [Fact]
    public async Task NotEmpty_MultiRunWithSameData_TablesAreCorrect()
    {
        // setup
        await using var disposableDatabase = await SetupNewDbAsync();
        var indexer = GetIndexer(disposableDatabase);
        
        using var rootPath = new DisposableDirectory();
        var file1 = rootPath.CreateFile();
        var directory1 = rootPath.CreateDirectory();
        var directory1File2 = directory1.CreateFile();
        // act
        
        var channel = Channel.CreateUnbounded<FileSystemInfo>();
        var readTask = indexer.RunAsync(channel.Reader, CancellationToken.None);

        channel.Writer.TryWrite(file1);
        channel.Writer.TryWrite(directory1File2);
        channel.Writer.TryWrite(directory1.Info);
        channel.Writer.Complete();
        await readTask;
        
        var channel2 = Channel.CreateUnbounded<FileSystemInfo>();
        await Task.Delay(1000);
        var startTime = DateTime.UtcNow;
        var readTask2 = indexer.RunAsync(channel2.Reader, CancellationToken.None);

        channel2.Writer.TryWrite(file1);
        channel2.Writer.TryWrite(directory1File2);
        channel2.Writer.TryWrite(directory1.Info);
        channel2.Writer.Complete();
        await readTask2;
        
        // verify
        var files = await GetAllFilesAsync(disposableDatabase.DbConnectionString);
        var directories = await GetAllDirectoriesAsync(disposableDatabase.DbConnectionString);

        files.Should().HaveCount(2);
        directories.Should().HaveCount(1);

        var file1Assert = files.Should().ContainKey(NormalizePath(file1.FullName))
            .WhoseValue;
        file1Assert.Name
            .Should()
            .Be(file1.Name);
        file1Assert.Path
            .Should()
            .Be(NormalizePath(file1.FullName));
        file1Assert.ParentPath
            .Should()
            .Be(NormalizePath(rootPath.Value));
        file1Assert.Size
            .Should()
            .Be(file1.Length);
        file1Assert.LastModified
            .Should()
            .NotBeAfter(startTime);
        
        var file2Assert = files.Should().ContainKey(NormalizePath(directory1File2.FullName))
            .WhoseValue;
        file2Assert.Name
            .Should()
            .Be(directory1File2.Name);
        file2Assert.Path
            .Should()
            .Be(NormalizePath(directory1File2.FullName));
        file2Assert.ParentPath
            .Should()
            .Be(NormalizePath(directory1.Value));
        file2Assert.Size
            .Should()
            .Be(directory1File2.Length);
        file2Assert.LastModified
            .Should()
            .BeBefore(startTime);

        var directory1Assert = directories.Should().ContainKey(NormalizePath(directory1.Value))
            .WhoseValue;

        directory1Assert.Name
            .Should()
            .Be(directory1.Info.Name);
        directory1Assert.Path
            .Should()
            .Be(NormalizePath(directory1.Value));
        directory1Assert.ParentPath
            .Should()
            .Be(NormalizePath(rootPath.Value));
        directory1Assert.LastModified
            .Should()
            .BeBefore(startTime);
    }

    [Fact]
    public async Task NotEmpty_MultiRunWithUpdates_TablesAreCorrect()
    {
        // setup
        await using var disposableDatabase = await SetupNewDbAsync();
        var indexer = GetIndexer(disposableDatabase);

        using var rootPath = new DisposableDirectory();
        var file1 = rootPath.CreateFile();
        var directory1 = rootPath.CreateDirectory();
        var directory1File2 = directory1.CreateFile();

        var channel = Channel.CreateUnbounded<FileSystemInfo>();
        var readTask = indexer.RunAsync(channel.Reader, CancellationToken.None);

        channel.Writer.TryWrite(file1);
        channel.Writer.TryWrite(directory1File2);
        channel.Writer.TryWrite(directory1.Info);
        channel.Writer.Complete();
        await readTask;

        // modify
        await File.WriteAllTextAsync(file1.FullName, "");
        file1 = new FileInfo(file1.FullName);
        await File.WriteAllTextAsync(directory1File2.FullName, "");
        directory1File2 = new FileInfo(directory1File2.FullName);
        
        var startTime = DateTime.UtcNow;

        var channel2 = Channel.CreateUnbounded<FileSystemInfo>();
        await Task.Delay(1000);
        var readTask2 = indexer.RunAsync(channel2.Reader, CancellationToken.None);

        channel2.Writer.TryWrite(file1);
        channel2.Writer.TryWrite(directory1File2);
        channel2.Writer.TryWrite(directory1.Info);
        channel2.Writer.Complete();
        await readTask2;

        // verify
        var files = await GetAllFilesAsync(disposableDatabase.DbConnectionString);
        var directories = await GetAllDirectoriesAsync(disposableDatabase.DbConnectionString);

        files.Should().HaveCount(2);
        directories.Should().HaveCount(1);
        
        var file1Assert = files.Should().ContainKey(NormalizePath(file1.FullName))
            .WhoseValue;
        file1Assert.Name
            .Should()
            .Be(file1.Name);
        file1Assert.Path
            .Should()
            .Be(NormalizePath(file1.FullName));
        file1Assert.ParentPath
            .Should()
            .Be(NormalizePath(rootPath.Value));
        file1Assert.Size
            .Should()
            .Be(file1.Length);
        file1Assert.LastModified
            .Should()
            .BeAfter(startTime);

        var file2Assert = files.Should()
            .ContainKey(NormalizePath(directory1File2.FullName))
            .WhoseValue;
        
        file2Assert.Name
            .Should()
            .Be(directory1File2.Name);
        file2Assert.Path
            .Should()
            .Be(NormalizePath(directory1File2.FullName));
        file2Assert.ParentPath
            .Should()
            .Be(NormalizePath(directory1.Value));
        file2Assert.Size
            .Should()
            .Be(directory1File2.Length);
        file2Assert.LastModified
            .Should()
            .BeAfter(startTime);

        var directory1Assert = directories.Should()
            .ContainKey(NormalizePath(directory1.Value))
            .WhoseValue;

        directory1Assert.Name
            .Should()
            .Be(directory1.Info.Name);
        directory1Assert.Path
            .Should()
            .Be(NormalizePath(directory1.Value));
        directory1Assert.ParentPath
            .Should()
            .Be(NormalizePath(rootPath.Value));
        directory1Assert.LastModified
            .Should()
            .BeBefore(startTime);
    }

    [Fact]
    public async Task NotEmpty_MultiRunWithAdditions_TablesAreCorrect()
    {
        // setup
        await using var disposableDatabase = await SetupNewDbAsync();
        var indexer = GetIndexer(disposableDatabase);

        using var rootPath = new DisposableDirectory();
        var file1 = rootPath.CreateFile();
        var directory1 = rootPath.CreateDirectory();
        var directory1File2 = directory1.CreateFile();
            
        var channel = Channel.CreateUnbounded<FileSystemInfo>();
        var readTask = indexer.RunAsync(channel.Reader, CancellationToken.None);

        channel.Writer.TryWrite(file1);
        channel.Writer.TryWrite(directory1File2);
        channel.Writer.TryWrite(directory1.Info);
        channel.Writer.Complete();
        await readTask;
        
        var directory2 = rootPath.CreateDirectory();
        var directory2File3 = directory2.CreateFile();

        var channel2 = Channel.CreateUnbounded<FileSystemInfo>();
        
        var startTime = DateTime.UtcNow;
        var readTask2 = indexer.RunAsync(channel2.Reader, CancellationToken.None);

        channel2.Writer.TryWrite(file1);
        channel2.Writer.TryWrite(directory2.Info);
        channel2.Writer.TryWrite(directory2File3);
        channel2.Writer.TryWrite(directory1File2);
        channel2.Writer.TryWrite(directory1.Info);
        channel2.Writer.Complete();
        await readTask2;

        // verify
        var files = await GetAllFilesAsync(disposableDatabase.DbConnectionString);
        var directories = await GetAllDirectoriesAsync(disposableDatabase.DbConnectionString);
            
        
        files.Should().HaveCount(3);
        directories.Should().HaveCount(2);
        
        var file1Assert = files.Should().ContainKey(NormalizePath(file1.FullName))
            .WhoseValue;
        file1Assert.Name
            .Should()
            .Be(file1.Name);
        file1Assert.Path
            .Should()
            .Be(NormalizePath(file1.FullName));
        file1Assert.ParentPath
            .Should()
            .Be(NormalizePath(rootPath.Value));
        file1Assert.Size
            .Should()
            .Be(file1.Length);
        file1Assert.LastModified
            .Should()
            .BeBefore(startTime);

        var file2Assert = files.Should().ContainKey(NormalizePath(directory1File2.FullName))
            .WhoseValue;
        file2Assert.Name
            .Should()
            .Be(directory1File2.Name);
        file2Assert.Path
            .Should()
            .Be(NormalizePath(directory1File2.FullName));
        file2Assert.ParentPath
            .Should()
            .Be(NormalizePath(directory1.Value));
        file2Assert.Size
            .Should()
            .Be(directory1File2.Length);
        file2Assert.LastModified
            .Should()
            .BeBefore(startTime);

        var directory1Assert = directories.Should().ContainKey(NormalizePath(directory1.Value))
            .WhoseValue;

        directory1Assert.Name
            .Should()
            .Be(directory1.Info.Name);
        directory1Assert.Path
            .Should()
            .Be(NormalizePath(directory1.Value));
        directory1Assert.ParentPath
            .Should()
            .Be(NormalizePath(rootPath.Value));
        directory1Assert.LastModified
            .Should()
            .BeBefore(startTime);
            
        var file3Assert = files.Should().ContainKey(NormalizePath(directory2File3.FullName))
            .WhoseValue;
        file3Assert.Name
            .Should()
            .Be(directory2File3.Name);
        file3Assert.Path
            .Should()
            .Be(NormalizePath(directory2File3.FullName));
        file3Assert.ParentPath
            .Should()
            .Be(NormalizePath(directory2.Value));
        file3Assert.Size
            .Should()
            .Be(directory2File3.Length);
        file3Assert.LastModified
            .Should()
            .BeAfter(startTime);

        var directory2Assert = directories.Should().ContainKey(NormalizePath(directory2.Value))
            .WhoseValue;

        directory2Assert.Name
            .Should()
            .Be(directory2.Info.Name);
        directory2Assert.Path.
            Should()
            .Be(NormalizePath(directory2.Value));
        directory2Assert.ParentPath
            .Should()
            .Be(NormalizePath(rootPath.Value));
        directory2Assert.LastModified
            .Should()
            .BeAfter(startTime);
    }
    
    [Fact]
    public async Task VerifyFilesTableSchema()
    {
        await using var disposableDatabase = await SetupNewDbAsync();

        var filesColumns = await GetColsAsync(disposableDatabase.DbConnectionString, "Files");

        var pathColumn = filesColumns
            .Should()
            .ContainKey("Path")
            .WhoseValue;
        
        pathColumn
            .DataType
            .Should()
            .Be("varchar");
        
        pathColumn
            .MaxLength
            .Should()
            .Be(256);

        pathColumn
            .IsNullable
            .Should()
            .BeFalse();
        
        var nameColumn = filesColumns
            .Should()
            .ContainKey("Name")
            .WhoseValue;
        
        nameColumn
            .DataType
            .Should()
            .Be("varchar");
        
        nameColumn
            .MaxLength
            .Should()
            .Be(256);

        nameColumn
            .IsNullable
            .Should()
            .BeFalse();
        
        var parentPathColumn = filesColumns
            .Should()
            .ContainKey("DirectoryPath")
            .WhoseValue;
        
        parentPathColumn
            .DataType
            .Should()
            .Be("varchar");
        
        parentPathColumn
            .MaxLength
            .Should()
            .Be(256);

        parentPathColumn
            .IsNullable
            .Should()
            .BeFalse();

        var sizeColumn = filesColumns
            .Should()
            .ContainKey("Size")
            .WhoseValue;
        
        sizeColumn
            .DataType
            .Should()
            .Be("bigint");
        
        sizeColumn
            .IsNullable
            .Should()
            .BeTrue();
        
        var lastModifiedColumn = filesColumns
            .Should()
            .ContainKey("LastModified")
            .WhoseValue;
        
        lastModifiedColumn
            .DataType
            .Should()
            .Be("datetime");

        lastModifiedColumn
            .IsNullable
            .Should()
            .BeFalse();
    }

    [Fact]
    public async Task VerifyDirectoriesTableSchema()
    {
        await using var disposableDatabase = await SetupNewDbAsync();
        var directoriesColumns = await GetColsAsync(disposableDatabase.DbConnectionString, "Directories");

        var pathColumn = directoriesColumns
            .Should()
            .ContainKey("Path")
            .WhoseValue;
        
        pathColumn
            .DataType
            .Should()
            .Be("varchar");
        
        pathColumn
            .MaxLength
            .Should()
            .Be(256);

        pathColumn
            .IsNullable
            .Should()
            .BeFalse();
        
        var nameColumn = directoriesColumns
            .Should()
            .ContainKey("Name")
            .WhoseValue;
        
        nameColumn
            .DataType
            .Should()
            .Be("varchar");
        
        nameColumn
            .MaxLength
            .Should()
            .Be(256);

        nameColumn
            .IsNullable
            .Should()
            .BeFalse();
        
        var parentPathColumn = directoriesColumns
            .Should()
            .ContainKey("DirectoryPath")
            .WhoseValue;
        
        parentPathColumn
            .DataType
            .Should()
            .Be("varchar");
        
        parentPathColumn
            .MaxLength
            .Should()
            .Be(256);

        parentPathColumn
            .IsNullable
            .Should()
            .BeFalse();

        var lastModifiedColumn = directoriesColumns
            .Should()
            .ContainKey("LastModified")
            .WhoseValue;
        
        lastModifiedColumn
            .DataType
            .Should()
            .Be("datetime");

        lastModifiedColumn
            .IsNullable
            .Should()
            .BeFalse();
    }

    private async Task<Dictionary<string, FileEntity>> GetAllFilesAsync(string connectionString)
    {
        await using SqlConnection connection =
            new(connectionString);
        await connection.OpenAsync();
        var command = new SqlCommand("SELECT * FROM Files", connection);
        await using var reader = await command.ExecuteReaderAsync();
        var result = new Dictionary<string, FileEntity>();
        while (await reader.ReadAsync())
        {
            var path = reader.GetString("Path");
            var name = reader.GetString("Name");
            var parentPath = reader.GetString("DirectoryPath");
            var size = reader.GetInt64("Size");
            var lastModified = reader.GetDateTime("LastModified");
            result.Add(NormalizePath(path), new FileEntity(NormalizePath(path), name, NormalizePath(parentPath), size, lastModified));
        }

        return result;
    }
    
    private async Task<Dictionary<string, DirectoryEntity>> GetAllDirectoriesAsync(string connectionString)
    {
        await using SqlConnection connection =
            new(connectionString);
        await connection.OpenAsync();
        var command = new SqlCommand("SELECT * FROM Directories", connection);
        await using var reader = await command.ExecuteReaderAsync();
        var result = new Dictionary<string, DirectoryEntity>();
        while (await reader.ReadAsync())
        {
            var path = reader.GetString("Path");
            var name = reader.GetString("Name");
            var parentPath = reader.GetString("DirectoryPath");
            var lastModified = reader.GetDateTime("LastModified");
            result.Add(NormalizePath(path), new DirectoryEntity(NormalizePath(path), name, NormalizePath(parentPath), lastModified));
        }

        return result;
    }
    
    private async Task<Dictionary<string, DbColumnEntity>> GetColsAsync(string connectionString, string tableName)
    {
        await using SqlConnection connection =
            new(connectionString);
        await connection.OpenAsync();
        var restrictions = new string[4];
        restrictions[2] = tableName;
        var table = connection.GetSchema("Columns",restrictions);

        return table.Rows.OfType<DataRow>()
            .Select(DbColumnEntity.FromDataRow)
            .ToDictionary(x => x.Name, x => x);
    }
    private string NormalizePath(string path)
    {
        if (path.IsNullOrEmpty())
        {
            return path;
        }
        return Path.GetFullPath(path).TrimEnd('\\');
    }

    private async Task<DisposableDatabase> SetupNewDbAsync()
    {
        var dbName = $"test_{Guid.NewGuid().ToString()[..8]}";
        var result = new DisposableDatabase(ServerConnectionString, dbName);
        try
        {
            await using(var connection = new SqlConnection(ServerConnectionString))
            {
                await using SqlCommand command = new($"CREATE DATABASE {dbName};", connection);

                await connection.OpenAsync();
                TestOutputHelper.WriteLine("Creating Database... Name: {0}", dbName);
                await command.ExecuteNonQueryAsync();
            }

            var newConnectionBuilder = new SqlConnectionStringBuilder(ServerConnectionString)
            {
                InitialCatalog = dbName
            };
            await MigrationRunner.RunAsync(newConnectionBuilder.ConnectionString);
            result.DbConnectionString = newConnectionBuilder.ConnectionString;
            return result;
        }
        catch (Exception ex)
        {
            await result.DisposeAsync();
            
            throw;
        }
    }
    
    private static GalacticSqlServerIndexer GetIndexer(DisposableDatabase database) =>
        new(new NullLogger<GalacticSqlServerIndexer>(),
            new OptionsWrapper<GalacticOptions>(new GalacticOptions
            {
                ConnectionString = database.DbConnectionString
            }));

    public async Task InitializeAsync()
    {
        await _sqlServerContainer.StartAsync();
    }

    public async Task DisposeAsync()
    {
        await _sqlServerContainer.DisposeAsync();
    }
}