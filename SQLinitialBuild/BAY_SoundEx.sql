USE [MCN_Connect]
GO

/****** Object:  Table [dbo].[BAY_SoundEx]    Script Date: 6/22/2022 7:30:38 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[BAY_SoundEx](
	[id_num] [int] NOT NULL,
	[first_name] [varchar](30) NULL,
	[first_sdx] [varchar](10) NULL,
	[last_name] [varchar](60) NULL,
	[last_sdx] [varchar](10) NULL,
 CONSTRAINT [PK_BAY_SoundEx] PRIMARY KEY CLUSTERED 
(
	[id_num] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO


