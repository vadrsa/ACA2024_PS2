Create Table Directories(
Id INT IDENTITY(1,1) PRIMARY KEY,
[Path]VARCHAR(256) NOT NULL,
[DirectoryPath] VARCHAR(256) NOT NULL,
[Name] VARCHAR(256) NOT NULL,
LastModified DATETIME NOT NULL
)
Create Table Files(
Id int identity(1,1) Primary key,
[Path] VARCHAR(256) NOT NULL,
[DirectoryPath] VARCHAR(256) NOT NULL,
[Name] VARCHAR(256) NOT NULL,
LastModified DATETIME NOT NULL,
Size BIGINT NOT NULL,
DirectoryId INT FOREIGN KEY REFERENCES Directories(Id)
)

CREATE INDEX Directory_Path
ON Directories([Path])

CREATE INDEX File_Path
ON Files([Path])
