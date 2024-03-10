CREATE TABLE Directories (
    Directory_id INT Identity(1, 1) PRIMARY KEY,
    [Path] VARCHAR(256) NOT NULL,
    [Name] VARCHAR(256) NOT NULL,
    DirectoryPath VARCHAR(256) NOT NULL,
    LastModified DATETIME NOT NULL
);

CREATE TABLE Files (
    File_id INT Identity(1, 1) PRIMARY KEY,
    [Path] VARCHAR(256) NOT NULL,
    [Name] VARCHAR(256) NOT NULL,
    DirectoryPath VARCHAR(256) NOT NULL,
    [Size] BIGINT,
    LastModified DATETIME NOT NULL
);

CREATE INDEX idx_Directories_Name ON Directories([Name]);
CREATE INDEX idx_Directories_Path ON Directories([Path]);

CREATE INDEX idx_Files_Name ON Files([Name]);
CREATE INDEX idx_Files_Path ON Files([Path]);