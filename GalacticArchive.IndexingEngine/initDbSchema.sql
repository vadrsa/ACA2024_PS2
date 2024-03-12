CREATE DATABASE GalacticArchive;
USE GalacticArchive;

CREATE TABLE Directories
(
Path varchar(256) primary key not null,
Name varchar(256)  not null,
DirectoryPath varchar(256) not null,
LastModified datetime not null, 
);

CREATE NONCLUSTERED INDEX IX_Directories_Name ON Directories (Name);

CREATE TABLE Files
(
Path varchar(256) primary key not null,
Name varchar(256)  not null,
DirectoryPath varchar(256) not null,
Size bigint,
LastModified datetime not null, 
);

CREATE NONCLUSTERED INDEX IX_Files_Name ON Files (Name);

