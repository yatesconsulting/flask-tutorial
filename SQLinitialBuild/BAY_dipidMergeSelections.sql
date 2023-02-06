USE [MCN_Connect]
GO

/****** Object:  Table [dbo].[BAY_dipidMergeSelections]    Script Date: 6/22/2022 7:29:55 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[BAY_dipidMergeSelections](
	[sql] [varchar](max) NOT NULL,
	[dupset] [int] NOT NULL,
	[tablename] [varchar](50) NOT NULL,
	[xkeys] [varchar](4000) NOT NULL,
	[updated] [datetime] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO

ALTER TABLE [dbo].[BAY_dipidMergeSelections] ADD  CONSTRAINT [DF_BAY_dipidMergeSelections_updated]  DEFAULT (getdate()) FOR [updated]
GO


