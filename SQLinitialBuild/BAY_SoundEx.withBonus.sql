USE [MCN_Connect]
GO

/****** Object:  Table [dbo].[BAY_SoundEx]    Script Date: 6/22/2022 7:30:38 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


-- Not sure what to think of built in SOUNDEX_TABLE table, so maybe this could be helpful
-- to identify dups based on similar sounding first+last names

-- didn't find this helpful, but didn't work too hard on figuring out built in methods
--select soundex_cde,count(*)  from tmseprd..SOUNDEX_TABLE
--where soundex_cde not like '%0000'
--group by soundex_cde
--having count(*) > 1

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

delete from BAY_SoundEx
insert into BAY_SoundEx
(id_num, first_name, first_sdx,last_name, last_sdx) 
select  id_num, FIRST_NAME,SOUNDEX(trim(First_name)) as FIRST_SDX
, LAST_NAME
, SOUNDEX(trim(last_name)) as LAST_SDX
from tmseply..NAMEMASTER
-- where ID_NUM > 4300000 and ID_NUM < 4399999
where name_type = 'P'

select
  first_sdx
, last_sdx
, min(id_num) as LowID
, max(id_num) as HighID
, count(*) as cnt
from BAY_SoundEx
group by first_sdx,last_sdx
having count(*) > 1
order by first_sdx,last_sdx

-- and some generally bad storage habits
-- just copy the update statement just after the -- through the where clause when ready to update

select * from tmseply..namemaster
-- update tmseply..namemaster set FIRST_NAME = trim(FIRST_NAME)
-- update tmseprd..namemaster set FIRST_NAME = trim(FIRST_NAME)
where (FIRST_NAME like ' %' or first_name like '% ') and trim(FIRST_NAME) <> ''

select * from tmseply..namemaster
-- update tmseply..namemaster set LAST_NAME = trim(LAST_NAME)
-- update tmseprd..namemaster set LAST_NAME = trim(LAST_NAME)
where (LAST_NAME like ' %' or last_name like '% ') and trim(LAST_NAME) <> ''


