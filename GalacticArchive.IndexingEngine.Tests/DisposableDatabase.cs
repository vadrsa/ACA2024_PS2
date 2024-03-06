using System.Diagnostics;
using Microsoft.Data.SqlClient;

namespace GalacticArchive.IndexingEngine.Tests;

public class DisposableDatabase : IAsyncDisposable
{
    private readonly string _connectionString;
    private readonly string _dbName;

    public DisposableDatabase(string connectionString, string dbName)
    {
        _connectionString = connectionString;
        _dbName = dbName;
    }

    public string DbConnectionString { get; set; }

    public async ValueTask DisposeAsync()
    {
        try
        {
            await using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();
            SqlCommand command = new($"DROP DATABASE {_dbName};", connection);
            await command.ExecuteNonQueryAsync();
        }
        catch (Exception ex)
        {
            Debug.WriteLine($"Failed to cleanup the db: {_dbName}. Reason={ex}");
        }
    }
}