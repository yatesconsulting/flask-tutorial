use tmseply
USE MCN_Connect
-- both of these are hard coded later on

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- This table has the DupID sets, built from flask
CREATE TABLE [MCN_Connect]..[BAY_DupIDs](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[dupset] [int] NULL,
	[id_num] [int] NULL,
	[human_verified] [bit] NULL,
	[goodid] [int] NULL,
	[origtablewithdup] [varchar](50) NULL,
	[db] [varchar](10) NULL,
	[updated] [datetime] NULL,
 CONSTRAINT [PK_BAY_DupIDs] PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[BAY_DupIDs] ADD  CONSTRAINT [DF_BAY_DupIDs_updated]  DEFAULT (getdate()) FOR [updated]
GO

-- this table sets the basic duplciate key columns for each table, and lists all tables to search, 1 row per table
CREATE TABLE [dbo].[BAY_DupExtraKeys](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[tablename] [varchar](63) NULL,
	[xkeys] [varchar](255) NULL,
 CONSTRAINT [PK_BAY_DupExtraKeys] PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)) ON [PRIMARY]
GO

-- this populates the relative tables, and should be looked over closly after running once
insert into [MCN_Connect]..BAY_DupExtraKeys (tablename)
SELECT t.name AS table_name
    FROM tmsply.sys.tables AS t
    INNER JOIN tmsply.sys.columns c
    ON t.OBJECT_ID = c.OBJECT_ID
    WHERE c.name = 'id_num' and t.name not like 'bkp%'
GO

-- this table is used by flask to track form submission and detailed extrakey queries
CREATE TABLE [dbo].[BAY_DupsInProgress](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[db] [varchar](10) NOT NULL,
	[dupset] [int] NOT NULL,
	[tablename] [varchar](50) NOT NULL,
	[xkeys] [varchar](4000) NULL,
	[username] [varchar](50) NULL,
 CONSTRAINT [PK_BAY_DupsInProgress] PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

