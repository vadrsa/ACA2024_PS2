namespace GalacticArchive.IndexingEngine.Tests.Entities;

public class DirectoryEntity
{
    public DirectoryEntity(string path, string name, string parentPath, DateTime lastModified)
    {
        Path = path;
        Name = name;
        ParentPath = parentPath;
        LastModified = lastModified;
    }

    public string Path { get; set; }
    public string Name { get; set; }
    public string ParentPath { get; set; }
    public DateTime LastModified { get; set; }
}