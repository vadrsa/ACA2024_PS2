CREATE
DATABASE GalacticArchive;

-- Use the created database
USE
GalacticArchive;

-- Create Directories table
CREATE TABLE Directories
(
    Path          VARCHAR(256) NOT NULL,
    Name          VARCHAR(256) NOT NULL,
    DirectoryPath VARCHAR(256) NOT NULL,
    LastModified  DATETIME     NOT NULL,
    CONSTRAINT PK_Directories PRIMARY KEY (Path)
);

-- Create Files table
CREATE TABLE Files
(
    Path          VARCHAR(256) NOT NULL,
    Name          VARCHAR(256) NOT NULL,
    DirectoryPath VARCHAR(256) NOT NULL,
    Size          BIGINT,
    LastModified  DATETIME     NOT NULL,
    CONSTRAINT PK_Files PRIMARY KEY (Path)
);

-- Optional: Indexing for performance
CREATE INDEX IX_DirectoryPath ON Directories (DirectoryPath);
CREATE INDEX IX_LastModified_Directories ON Directories (LastModified);
CREATE INDEX IX_DirectoryPath_Files ON Files (DirectoryPath);
CREATE INDEX IX_LastModified_Files ON Files (LastModified);