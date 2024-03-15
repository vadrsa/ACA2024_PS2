CREATE TABLE Directories (
     Id INT Identity(1, 1) PRIMARY KEY,
     Path VARCHAR(256) NOT NULL,
     Name VARCHAR(256) NOT NULL,
     DirectoryPath VARCHAR(256) NOT NULL,
     LastModified DATETIME NOT NULL
);

CREATE TABLE Files (
   Id INT Identity(1, 1) PRIMARY KEY,
   Path VARCHAR(256) NOT NULL,
   Name VARCHAR(256) NOT NULL,
   DirectoryPath VARCHAR(256) NOT NULL,
   Size BIGINT,
   LastModified DATETIME NOT NULL
);

CREATE INDEX idx_SearchDirectoryByPath ON Directories(Path);
CREATE INDEX idx_SearchFileByPath ON Files(Path);