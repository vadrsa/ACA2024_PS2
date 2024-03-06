using Microsoft.Data.SqlClient;

namespace GalacticArchive;

public static class MigrationRunner
{
    public static async Task RunAsync(string connectionString)
    {
        var path = Path.Combine( 
            AppContext.BaseDirectory, "initDbSchema.sql");
        var initDbSchemaContent = await File.ReadAllTextAsync(path);
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        await using SqlCommand createSchemaCommand = new(initDbSchemaContent, connection);
        await createSchemaCommand.ExecuteNonQueryAsync();
    }
}