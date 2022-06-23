USE [MCN_Connect]
GO

/****** Object:  Table [dbo].[BAY_DupExtraKeys]    Script Date: 6/22/2022 7:30:01 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[BAY_DupExtraKeys](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[tablename] [varchar](63) NULL,
	[xkeys] [varchar](255) NULL,
	[tablekeyname] [varchar](20) NULL,
	[mykeyname] [varchar](20) NULL,
	[tableuniqkey] [varchar](127) NULL,
	[defaultaction] [varchar](20) NULL,
 CONSTRAINT [PK_BAY_DupExtraKeys] PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO


