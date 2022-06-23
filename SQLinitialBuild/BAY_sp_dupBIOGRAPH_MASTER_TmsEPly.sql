USE [MCN_Connect]
GO

/****** Object:  StoredProcedure [dbo].[BAY_sp_dupBIOGRAPH_MASTER_TmsEPly]    Script Date: 6/22/2022 8:38:58 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


-- =============================================
-- Author:		Bryan Yates
-- Create date: 10/10/2021
-- Description:	SSN dups identified and inserted into DupIDs table
-- =============================================
CREATE PROCEDURE [dbo].[BAY_sp_dupBIOGRAPH_MASTER_TmsEPly]
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

    -- Insert statements for procedure here
	-- dup SSN is probably a duplicate person
DECLARE @ssn int 
DECLARE @dupset int
SELECT @dupset = max(dupset) + 1  from mcn_connect..BAY_DupIDs

DECLARE db_cursor CURSOR FOR 
select 
 SSN
from TmsEPly..BIOGRAPH_MASTER
where ssn > 0
group by ssn
having count(*) > 1

OPEN db_cursor  
FETCH NEXT FROM db_cursor INTO @ssn  

WHILE @@FETCH_STATUS = 0  
BEGIN  
  insert into MCN_Connect..BAY_DupIDs 
    (id_num, human_verified, origtablewithdup ,dupset, db)
  select id_num, 0, 'BIOGRAPH_MASTER', @dupset, 'TmsEPly' 
  from TmsEPly..BIOGRAPH_MASTER where ssn = @ssn
  set @dupset = @dupset + 1
  FETCH NEXT FROM db_cursor INTO @ssn 
END 
CLOSE db_cursor  
DEALLOCATE db_cursor 

END
GO


