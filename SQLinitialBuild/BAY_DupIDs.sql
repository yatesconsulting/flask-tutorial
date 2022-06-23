USE [MCN_Connect]
GO

/****** Object:  Table [dbo].[BAY_DupIDs]    Script Date: 6/22/2022 7:30:08 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[BAY_DupIDs](
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


