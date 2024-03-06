using System.Diagnostics;
using Microsoft.Data.SqlClient;

namespace GalacticArchive.IndexingEngine.Tests;

public class DirectoryWrapper
{
    protected DirectoryWrapper(string value)
    {
        Value = value;
        Directory.CreateDirectory(Value);
        Info = new DirectoryInfo(Value);
    }
    
    public string Value { get; }
    public DirectoryInfo Info { get; }

    public FileInfo CreateFile()
    {
        var name = $"{Guid.NewGuid()}.txt";
        var path = Path.Combine(Value, name);
        File.WriteAllText(path, name);
        return new FileInfo(path);
    }
    
    public DirectoryWrapper CreateDirectory()
    {
        var name = Guid.NewGuid().ToString();
        var path = Path.Combine(Value, name);
        Directory.CreateDirectory(path);
        return new DirectoryWrapper(path);
    }
}

public class DisposableDirectory : DirectoryWrapper, IDisposable
{
    public DisposableDirectory() : base(Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString()))
    {
    }

    public void Dispose()
    {
        Directory.Delete(Value, recursive: true);
    }
}