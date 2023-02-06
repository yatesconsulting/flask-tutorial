USE [MCN_Connect]
GO

/****** Object:  Table [dbo].[BAY_DupsInProgress]    Script Date: 6/22/2022 7:30:13 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[BAY_DupsInProgress](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[db] [varchar](10) NOT NULL,
	[dupset] [int] NOT NULL,
	[tablename] [varchar](50) NOT NULL,
	[tablekeyname] [varchar](20) NULL,
	[mykeyname] [varchar](20) NULL,
	[xkeys] [varchar](4000) NULL,
	[cnt] [varchar](200) NULL,
	[category] [varchar](30) NULL,
	[updated] [smalldatetime] NULL,
 CONSTRAINT [PK_BAY_DupsInProgress] PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[BAY_DupsInProgress] ADD  CONSTRAINT [DF_BAY_DupsInProgress_updated]  DEFAULT (getdate()) FOR [updated]
GO


