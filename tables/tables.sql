-- Create table for OrganizationalUnits (Parent Table)
CREATE TABLE OrganizationalUnits (
    OrgUnitId INT PRIMARY KEY,
    Name VARCHAR(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    Code VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    IsActive BOOLEAN,
    CreatedDate DATETIME,
    IsDeleted BOOLEAN,
    Year INT,
    Term VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    Duration VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    Section VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    Department VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    CourseNumber VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    SectionType VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
) ENGINE=InnoDB CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Create table for ContentObjects (Child Table)
CREATE TABLE ContentObjects (
    ContentObjectId INT PRIMARY KEY,
    OrgUnitId INT NOT NULL,
    Title VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    ContentObjectType VARCHAR(6) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    Location VARCHAR(1024) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
    LastModified DATETIME,
    IsDeleted BOOLEAN,
    Recorded BOOLEAN
) ENGINE=InnoDB CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Recreate it without foreign key constraints
CREATE TABLE OrganizationalUnitAncestors (
    OrgUnitId INT NOT NULL,
    AncestorOrgUnitId INT NOT NULL,
    PRIMARY KEY (OrgUnitId, AncestorOrgUnitId)
) ENGINE=InnoDB CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- CREATE TABLE Faculty (
--     FacultyId INT PRIMARY KEY,
--     Name VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
--     ProjectId INT
-- ) ENGINE=InnoDB CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- CREATE TABLE Syllabus (
--     OrgUnitId INT PRIMARY KEY,  
--     Name VARCHAR(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
--     Code VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
--     IsActive BOOLEAN,
--     CreatedDate DATETIME,
--     IsDeletedOrg BOOLEAN,
--     Year INT,
--     Term VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
--     Duration VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
--     Section VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
--     Department VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
--     CourseNumber VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
--     SectionType VARCHAR(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
--     ContentObjectId INT,
--     Title VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
--     ContentObjectType VARCHAR(6) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
--     Location VARCHAR(1024) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
--     LastModified DATETIME,
--     IsDeletedContent BOOLEAN,
--     SyllabusRecorded BOOLEAN
-- ) ENGINE=InnoDB CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;