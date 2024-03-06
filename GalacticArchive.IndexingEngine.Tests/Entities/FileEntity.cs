namespace GalacticArchive.IndexingEngine.Tests.Entities;

public class FileEntity
{
    public FileEntity(string path, string name, string parentPath, long size, DateTime lastModified)
    {
        Path = path;
        Name = name;
        ParentPath = parentPath;
        Size = size;
        LastModified = lastModified;
    }

    public string Path { get; set; }
    public string Name { get; set; }
    public string ParentPath { get; set; }
    public long Size { get; set; }
    public DateTime LastModified { get; set; }
}