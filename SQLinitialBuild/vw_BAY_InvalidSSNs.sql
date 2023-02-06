USE [MCN_Connect]
GO

/****** Object:  View [dbo].[BAY_InvalidSSNs]    Script Date: 6/22/2022 8:37:29 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE VIEW [dbo].[BAY_InvalidSSNs]
AS

select 
case when cast(ssn as varchar(20)) like '%'+cast(id_num as varchar(20))+'%'
then 'Yes' else 'No' end as IDinSSN
, case when cast (ssn as varchar(10)) like '9%' then '9__-__-____'
  when  cast (ssn as varchar(10)) like '666%' then '666-__-____'
  when len(cast (ssn as varchar(10))) < 6 then '000-__-____'
  when cast (ssn as varchar(10)) like '___00____' then '___-00-____'
-- SSA will not issue SSNs with the number “0000” in positions 6 – 9.
  when cast (ssn as varchar(10)) like '%0000' then '___-__-0000'
  end as WhyInvalid
  ,* from TmsEPly.dbo.BIOGRAPH_MASTER
where 
-- from pdf
-- SSA will not issue SSNs beginning with the number “9”.
cast (ssn as varchar(10)) like '9%'
-- SSA will not issue SSNs beginning with the number “666” in positions 1 – 3.
or cast (ssn as varchar(10)) like '666%'
-- SSA will not issue SSNs beginning with the number “000” in positions 1 – 3.
or len(cast (ssn as varchar(10))) < 6
-- SSA will not issue SSNs with the number “00” in positions 4 – 5.
or cast (ssn as varchar(10)) like '___00____'
-- SSA will not issue SSNs with the number “0000” in positions 6 – 9.
or cast (ssn as varchar(10)) like '%0000'

GO

EXEC sys.sp_addextendedproperty @name=N'MS_DiagramPane1', @value=N'[0E232FF0-B466-11cf-A24F-00AA00A3EFFF, 1.00]
Begin DesignProperties = 
   Begin PaneConfigurations = 
      Begin PaneConfiguration = 0
         NumPanes = 4
         Configuration = "(H (1[40] 4[20] 2[20] 3) )"
      End
      Begin PaneConfiguration = 1
         NumPanes = 3
         Configuration = "(H (1 [50] 4 [25] 3))"
      End
      Begin PaneConfiguration = 2
         NumPanes = 3
         Configuration = "(H (1 [50] 2 [25] 3))"
      End
      Begin PaneConfiguration = 3
         NumPanes = 3
         Configuration = "(H (4 [30] 2 [40] 3))"
      End
      Begin PaneConfiguration = 4
         NumPanes = 2
         Configuration = "(H (1 [56] 3))"
      End
      Begin PaneConfiguration = 5
         NumPanes = 2
         Configuration = "(H (2 [66] 3))"
      End
      Begin PaneConfiguration = 6
         NumPanes = 2
         Configuration = "(H (4 [50] 3))"
      End
      Begin PaneConfiguration = 7
         NumPanes = 1
         Configuration = "(V (3))"
      End
      Begin PaneConfiguration = 8
         NumPanes = 3
         Configuration = "(H (1[56] 4[18] 2) )"
      End
      Begin PaneConfiguration = 9
         NumPanes = 2
         Configuration = "(H (1 [75] 4))"
      End
      Begin PaneConfiguration = 10
         NumPanes = 2
         Configuration = "(H (1[66] 2) )"
      End
      Begin PaneConfiguration = 11
         NumPanes = 2
         Configuration = "(H (4 [60] 2))"
      End
      Begin PaneConfiguration = 12
         NumPanes = 1
         Configuration = "(H (1) )"
      End
      Begin PaneConfiguration = 13
         NumPanes = 1
         Configuration = "(V (4))"
      End
      Begin PaneConfiguration = 14
         NumPanes = 1
         Configuration = "(V (2))"
      End
      ActivePaneConfig = 0
   End
   Begin DiagramPane = 
      Begin Origin = 
         Top = 0
         Left = 0
      End
      Begin Tables = 
         Begin Table = "BIOGRAPH_MASTER (TmsEPrd.dbo)"
            Begin Extent = 
               Top = 6
               Left = 38
               Bottom = 136
               Right = 300
            End
            DisplayFlags = 280
            TopColumn = 0
         End
      End
   End
   Begin SQLPane = 
   End
   Begin DataPane = 
      Begin ParameterDefaults = ""
      End
   End
   Begin CriteriaPane = 
      Begin ColumnWidths = 13
         Column = 1440
         Alias = 900
         Table = 1170
         Output = 720
         Append = 1400
         NewValue = 1170
         SortType = 1350
         SortOrder = 1410
         GroupBy = 1350
         Filter = 1350
         Or = 1350
         Or = 1350
         Or = 1350
         Or = 1350
         Or = 1350
      End
   End
End
' , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'VIEW',@level1name=N'BAY_InvalidSSNs'
GO

EXEC sys.sp_addextendedproperty @name=N'MS_DiagramPaneCount', @value=1 , @level0type=N'SCHEMA',@level0name=N'dbo', @level1type=N'VIEW',@level1name=N'BAY_InvalidSSNs'
GO


