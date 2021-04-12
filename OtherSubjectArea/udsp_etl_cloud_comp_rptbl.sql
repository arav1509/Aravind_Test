CREATE OR REPLACE PROCEDURE `rax-abo-72-dev`.sales.udsp_etl_cloud_comp_rptbl()
begin

--- No proc executing
---------------------------------------------------------------------------------------
/*
Created On: 12/4/2013
Created By: Kano Cannick

Description:
Runs all the procs needed to populate report tables.


Modifications:
--------------

Modified By          Date     Description
-----------------  ---------- ----------------------------------------------------------


----------------------------------------------------------------------------------------
*/
----------------------------------------------------------------------------------------
DECLARE bgn_Key INT64;
DECLARE end_Key INT64;
----------------------------------------------------------------------------------------
SET @bgn_Key = dbo.udf_Time_KEY_NoHyphen(dateadd(d,-7,getdate()))
----------------------------------------------------------------------------------------
DELETE FROM	dbo.Report_Tables_Duration_Log WHERE Time_KEY <	@bgn_Key
DELETE FROM dbo.Report_Tables_Duration_Log WHERE Duration_Time = '00:00:00' OR Duration_Time = '1' AND [GROUP] in('PNR Billing Info','Cloud Opp Comp Load','Cloud Comp Delta Load','Invoice Reporting')
---------------------------------------------------------------------------------------
--udsp_etl_Cloud_Hosting_PNR_Invoiced_Detail
---------------------------------------------------------------------------------------
IF 
	ISNULL((SELECT MAX(Time_Key) FROM Report_Tables_Duration_Log WHERE [Procedure]='udsp_etl_Cloud_Hosting_PNR_Invoiced_Detail'),0)  <> dbo.udf_Time_KEY_NoHyphen(getdate())
BEGIN
INSERT INTO 
	Report_Tables_Duration_Log
SELECT
	[Table]='Cloud_Hosting_PNR_Invoiced_Detail',
	[Procedure]='udsp_etl_Cloud_Hosting_PNR_Invoiced_Detail',
	Time_Key=dbo.udf_Time_KEY_NoHyphen(getdate()),
	Start_Time= Getdate(),
	End_Time='1/1/1900',
	Duration_time='1',
	[GROUP]='PNR Billing Info'
---------------------------------------------------------------------------------------
--BEGIN
--EXEC udsp_etl_Cloud_Hosting_PNR_Invoiced_Detail
--END
--BEGIN
--EXEC udsp_Update_Cloud_ENT_IB_PNR_Comp_Account_Adjustments
--END
--BEGIN
--EXEC udsp_etl_Cloud_Hosting_PNR_Invoiced_Detail_Adjustments_Inserts
--END
--BEGIN
--EXEC udsp_Update_Cloud_Hosting_PNR_Invoiced_Detail
--END
---------------------------------------------------------------------------------------
Update 
	dbo.Report_Tables_Duration_Log 
SET 
	End_Time= Getdate()
WHERE 
	[Procedure]='udsp_etl_Cloud_Hosting_PNR_Invoiced_Detail' AND dbo.udf_Time_KEY_NoHyphen(Start_Time)=dbo.udf_Time_KEY_NoHyphen(getdate())
---------------------------------------------------------------------------------------
Update 
	Report_Tables_Duration_Log 
SET
	Duration_time=
(
CASE
	WHEN
		[dbo].[udfTimeInterval](CAST(ISNULL((DATEDIFF(second,Start_Time,End_Time)),0)as bigint)) < '00:00:01'
	THEN
		'00:00:02'
	ELSE	
		[dbo].[udfTimeInterval](CAST(ISNULL((DATEDIFF(second,Start_Time,End_Time)),0)as bigint))
END
)
WHERE 
	[Procedure]='udsp_etl_Cloud_Hosting_PNR_Invoiced_Detail' AND dbo.udf_Time_KEY_NoHyphen(Start_Time)=dbo.udf_Time_KEY_NoHyphen(getdate())
END	
----------------------------------------------------------------------------------------
--load udsp_etl_Cloud_ENT_ACQ_PNR_Comp_Data
----------------------------------------------------------------------------------------
IF 
	ISNULL((SELECT MAX(Time_Key) FROM Report_Tables_Duration_Log WHERE [Procedure]='udsp_etl_Cloud_ENT_ACQ_PNR_Comp_Data'),0)  <> dbo.udf_Time_KEY_NoHyphen(getdate())
BEGIN
INSERT INTO 
	 Report_Tables_Duration_Log  
SELECT
	[Table]='Cloud_ENT_ACQ_PNR_Comp_Data',
	[Procedure]='udsp_etl_Cloud_ENT_ACQ_PNR_Comp_Data',
	Time_Key=dbo.udf_Time_KEY_NoHyphen(getdate()),
	Start_Time= Getdate(),
	End_Time='1/1/1900',
	Duration_time='1',
	[GROUP]='Cloud Opp Comp Load'
---------------------------------------------------------------------------------------
--EXEC udsp_etl_Cloud_ENT_ACQ_PNR_Comp_Data
---------------------------------------------------------------------------------------
Update 
	dbo.Report_Tables_Duration_Log 
SET 
	End_Time= Getdate()
WHERE 
	[Procedure]='udsp_etl_Cloud_ENT_ACQ_PNR_Comp_Data' AND dbo.udf_Time_KEY_NoHyphen(Start_Time)=dbo.udf_Time_KEY_NoHyphen(getdate())
---------------------------------------------------------------------------------------
Update 
	Report_Tables_Duration_Log 
SET
	Duration_time=
(
CASE
	WHEN
		[dbo].[udfTimeInterval](CAST(ISNULL((DATEDIFF(second,Start_Time,End_Time)),0)as bigint)) < '00:00:01'
	THEN
		'00:00:02'
	ELSE	
		[dbo].[udfTimeInterval](CAST(ISNULL((DATEDIFF(second,Start_Time,End_Time)),0)as bigint))
END
)
WHERE 
	[Procedure]='udsp_etl_Cloud_ENT_ACQ_PNR_Comp_Data' AND dbo.udf_Time_KEY_NoHyphen(Start_Time)=dbo.udf_Time_KEY_NoHyphen(getdate())
END	

----------------------------------------------------------------------------------------
--load udsp_etl_Cloud_SMB_ACQ_PNR_Comp_Data
----------------------------------------------------------------------------------------
IF 
	ISNULL((SELECT MAX(Time_Key) FROM Report_Tables_Duration_Log WHERE [Procedure]='udsp_etl_Cloud_SMB_ACQ_PNR_Comp_Data'),0)  <> dbo.udf_Time_KEY_NoHyphen(getdate())
BEGIN
INSERT INTO 
	 Report_Tables_Duration_Log  
SELECT
	[Table]='Cloud_SMB_ACQ_PNR_Comp_Data',
	[Procedure]='udsp_etl_Cloud_SMB_ACQ_PNR_Comp_Data',
	Time_Key=dbo.udf_Time_KEY_NoHyphen(getdate()),
	Start_Time= Getdate(),
	End_Time='1/1/1900',
	Duration_time='1',
	[GROUP]='Cloud Opp Comp Load'
---------------------------------------------------------------------------------------
--EXEC udsp_etl_Cloud_SMB_ACQ_PNR_Comp_Data
---------------------------------------------------------------------------------------
Update 
	dbo.Report_Tables_Duration_Log 
SET 
	End_Time= Getdate()
WHERE 
	[Procedure]='udsp_etl_Cloud_SMB_ACQ_PNR_Comp_Data' AND dbo.udf_Time_KEY_NoHyphen(Start_Time)=dbo.udf_Time_KEY_NoHyphen(getdate())
---------------------------------------------------------------------------------------
Update 
	Report_Tables_Duration_Log 
SET
	Duration_time=
(
CASE
	WHEN
		[dbo].[udfTimeInterval](CAST(ISNULL((DATEDIFF(second,Start_Time,End_Time)),0)as bigint)) < '00:00:01'
	THEN
		'00:00:02'
	ELSE	
		[dbo].[udfTimeInterval](CAST(ISNULL((DATEDIFF(second,Start_Time,End_Time)),0)as bigint))
END
)
WHERE 
	[Procedure]='udsp_etl_Cloud_SMB_ACQ_PNR_Comp_Data' AND dbo.udf_Time_KEY_NoHyphen(Start_Time)=dbo.udf_Time_KEY_NoHyphen(getdate())
END	
----------------------------------------------------------------------------------------
--load udsp_etl_Cloud_ENT_IB_PNR_Comp_Data
----------------------------------------------------------------------------------------
IF 
	ISNULL((SELECT MAX(Time_Key) FROM Report_Tables_Duration_Log WHERE [Procedure]='udsp_etl_Cloud_ENT_IB_PNR_Comp_Data'),0)  <> dbo.udf_Time_KEY_NoHyphen(getdate())
BEGIN
INSERT INTO 
	 Report_Tables_Duration_Log  
SELECT
	[Table]='Cloud_ENT_IB_PNR_Comp_Data',
	[Procedure]='udsp_etl_Cloud_ENT_IB_PNR_Comp_Data',
	Time_Key=dbo.udf_Time_KEY_NoHyphen(getdate()),
	Start_Time= Getdate(),
	End_Time='1/1/1900',
	Duration_time='1',
	[GROUP]='Cloud Opp Comp Load'
---------------------------------------------------------------------------------------
--EXEC udsp_etl_Cloud_ENT_IB_PNR_Comp_Data
---------------------------------------------------------------------------------------
Update 
	dbo.Report_Tables_Duration_Log 
SET 
	End_Time= Getdate()
WHERE 
	[Procedure]='udsp_etl_Cloud_ENT_IB_PNR_Comp_Data' AND dbo.udf_Time_KEY_NoHyphen(Start_Time)=dbo.udf_Time_KEY_NoHyphen(getdate())
---------------------------------------------------------------------------------------
Update 
	Report_Tables_Duration_Log 
SET
	Duration_time=
(
CASE
	WHEN
		[dbo].[udfTimeInterval](CAST(ISNULL((DATEDIFF(second,Start_Time,End_Time)),0)as bigint)) < '00:00:01'
	THEN
		'00:00:02'
	ELSE	
		[dbo].[udfTimeInterval](CAST(ISNULL((DATEDIFF(second,Start_Time,End_Time)),0)as bigint))
END
)
WHERE 
	[Procedure]='udsp_etl_Cloud_ENT_IB_PNR_Comp_Data' AND dbo.udf_Time_KEY_NoHyphen(Start_Time)=dbo.udf_Time_KEY_NoHyphen(getdate())
END	

----------------------------------------------------------------------------------------
--load udsp_etl_Cloud_SMB_IB_PNR_Comp_Data
----------------------------------------------------------------------------------------
IF 
	ISNULL((SELECT MAX(Time_Key) FROM Report_Tables_Duration_Log WHERE [Procedure]='udsp_etl_Cloud_SMB_IB_PNR_Comp_Data'),0)  <> dbo.udf_Time_KEY_NoHyphen(getdate())
BEGIN
INSERT INTO 
	 Report_Tables_Duration_Log  
SELECT
	[Table]='Cloud_SMB_IB_PNR_Comp_Data',
	[Procedure]='udsp_etl_Cloud_SMB_IB_PNR_Comp_Data',
	Time_Key=dbo.udf_Time_KEY_NoHyphen(getdate()),
	Start_Time= Getdate(),
	End_Time='1/1/1900',
	Duration_time='1',
	[GROUP]='Cloud Opp Comp Load'
---------------------------------------------------------------------------------------
--EXEC udsp_etl_Cloud_SMB_IB_PNR_Comp_Data
---------------------------------------------------------------------------------------
Update 
	dbo.Report_Tables_Duration_Log 
SET 
	End_Time= Getdate()
WHERE 
	[Procedure]='udsp_etl_Cloud_SMB_IB_PNR_Comp_Data' AND dbo.udf_Time_KEY_NoHyphen(Start_Time)=dbo.udf_Time_KEY_NoHyphen(getdate())
---------------------------------------------------------------------------------------
Update 
	Report_Tables_Duration_Log 
SET
	Duration_time=
(
CASE
	WHEN
		[dbo].[udfTimeInterval](CAST(ISNULL((DATEDIFF(second,Start_Time,End_Time)),0)as bigint)) < '00:00:01'
	THEN
		'00:00:02'
	ELSE	
		[dbo].[udfTimeInterval](CAST(ISNULL((DATEDIFF(second,Start_Time,End_Time)),0)as bigint))
END
)
WHERE 
	[Procedure]='udsp_etl_Cloud_SMB_IB_PNR_Comp_Data' AND dbo.udf_Time_KEY_NoHyphen(Start_Time)=dbo.udf_Time_KEY_NoHyphen(getdate())
END	
----------------------------------------------------------------------------------------
--load udsp_etl_Cloud_Partner_PNR_Comp_Data
----------------------------------------------------------------------------------------
IF 
	ISNULL((SELECT MAX(Time_Key) FROM Report_Tables_Duration_Log WHERE [Procedure]='udsp_etl_Cloud_Partner_PNR_Comp_Data'),0)  <> dbo.udf_Time_KEY_NoHyphen(getdate())
BEGIN
INSERT INTO 
	 Report_Tables_Duration_Log  
SELECT
	[Table]='Cloud_Partner_PNR_Comp_Data',
	[Procedure]='udsp_etl_Cloud_Partner_PNR_Comp_Data',
	Time_Key=dbo.udf_Time_KEY_NoHyphen(getdate()),
	Start_Time= Getdate(),
	End_Time='1/1/1900',
	Duration_time='1',
	[GROUP]='Cloud Opp Comp Load'
---------------------------------------------------------------------------------------
----EXEC udsp_etl_Cloud_Partner_PNR_Comp_Data
---------------------------------------------------------------------------------------
Update 
	dbo.Report_Tables_Duration_Log 
SET 
	End_Time= Getdate()
WHERE 
	[Procedure]='udsp_etl_Cloud_Partner_PNR_Comp_Data' AND dbo.udf_Time_KEY_NoHyphen(Start_Time)=dbo.udf_Time_KEY_NoHyphen(getdate())
---------------------------------------------------------------------------------------
Update 
	Report_Tables_Duration_Log 
SET
	Duration_time=
(
CASE
	WHEN
		[dbo].[udfTimeInterval](CAST(ISNULL((DATEDIFF(second,Start_Time,End_Time)),0)as bigint)) < '00:00:01'
	THEN
		'00:00:02'
	ELSE	
		[dbo].[udfTimeInterval](CAST(ISNULL((DATEDIFF(second,Start_Time,End_Time)),0)as bigint))
END
)
WHERE 
	[Procedure]='udsp_etl_Cloud_Partner_PNR_Comp_Data' AND dbo.udf_Time_KEY_NoHyphen(Start_Time)=dbo.udf_Time_KEY_NoHyphen(getdate())
END	
----------------------------------------------------------------------------------------
--load Cloud_ENT_IB_PNR_Rolling_13_Month_Delta_Agg
----------------------------------------------------------------------------------------
IF 
	ISNULL((SELECT MAX(Time_Key) FROM Report_Tables_Duration_Log WHERE [Procedure]='udsp_etl_Cloud_ENT_IB_PNR_Rolling_13_Month_Delta_Agg'),0)  <> dbo.udf_Time_KEY_NoHyphen(getdate())
BEGIN
INSERT INTO 
	 Report_Tables_Duration_Log  
SELECT
	[Table]='Cloud_ENT_IB_PNR_Rolling_13_Month_Delta_Agg',
	[Procedure]='udsp_etl_Cloud_ENT_IB_PNR_Rolling_13_Month_Delta_Agg',
	Time_Key=dbo.udf_Time_KEY_NoHyphen(getdate()),
	Start_Time= Getdate(),
	End_Time='1/1/1900',
	Duration_time='1',
	[GROUP]='Cloud Comp Delta Load'
---------------------------------------------------------------------------------------
--BEGIN
--EXEC  udsp_etl_Cloud_ENT_IB_PNR_Rolling_13_Month_Delta_Agg_Drop_Index
--END
--BEGIN
--EXEC EXEC_udsp_etl_Cloud_ENT_IB_PNR_Rolling_13_Month_Delta_Agg
--END
--BEGIN
--EXEC udsp_etl_Cloud_ENT_IB_PNR_Rolling_13_Month_Delta_Agg_Create_Index
--END
---------------------------------------------------------------------------------------
Update 
	dbo.Report_Tables_Duration_Log 
SET 
	End_Time= Getdate()
WHERE 
	[Procedure]='udsp_etl_Cloud_ENT_IB_PNR_Rolling_13_Month_Delta_Agg' AND dbo.udf_Time_KEY_NoHyphen(Start_Time)=dbo.udf_Time_KEY_NoHyphen(getdate())
---------------------------------------------------------------------------------------
Update 
	Report_Tables_Duration_Log 
SET
	Duration_time=
(
CASE
	WHEN
		[dbo].[udfTimeInterval](CAST(ISNULL((DATEDIFF(second,Start_Time,End_Time)),0)as bigint)) < '00:00:01'
	THEN
		'00:00:02'
	ELSE	
		[dbo].[udfTimeInterval](CAST(ISNULL((DATEDIFF(second,Start_Time,End_Time)),0)as bigint))
END
)
WHERE 
	[Procedure]='udsp_etl_Cloud_ENT_IB_PNR_Rolling_13_Month_Delta_Agg' AND dbo.udf_Time_KEY_NoHyphen(Start_Time)=dbo.udf_Time_KEY_NoHyphen(getdate())
END	
----------------------------------------------------------------------------------------
--load Cloud_Partner_PNR_Rolling_13_Month_Delta_Agg
----------------------------------------------------------------------------------------
IF 
	ISNULL((SELECT MAX(Time_Key) FROM Report_Tables_Duration_Log WHERE [Procedure]='udsp_etl_Cloud_Partner_PNR_Rolling_13_Month_Delta_Agg'),0)  <> dbo.udf_Time_KEY_NoHyphen(getdate())
BEGIN
INSERT INTO 
	 Report_Tables_Duration_Log  
SELECT
	[Table]='Cloud_Partner_PNR_Rolling_13_Month_Delta_Agg',
	[Procedure]='udsp_etl_Cloud_Partner_PNR_Rolling_13_Month_Delta_Agg',
	Time_Key=dbo.udf_Time_KEY_NoHyphen(getdate()),
	Start_Time= Getdate(),
	End_Time='1/1/1900',
	Duration_time='1',
	[GROUP]='Cloud Comp Delta Load'
---------------------------------------------------------------------------------------
--BEGIN
--EXEC [udsp_etl_Cloud_Partner_PNR_Rolling_13_Month_Delta_Agg_Drop_Index]
--END
--BEGIN
--EXEC EXEC_udsp_etl_Cloud_Partner_PNR_Rolling_13_Month_Delta_Agg
--END
--BEGIN
--EXEC [udsp_etl_Cloud_Partner_PNR_Rolling_13_Month_Delta_Agg_Create_Index]
--END
---------------------------------------------------------------------------------------
Update 
	dbo.Report_Tables_Duration_Log 
SET 
	End_Time= Getdate()
WHERE 
	[Procedure]='udsp_etl_Cloud_Partner_PNR_Rolling_13_Month_Delta_Agg' AND dbo.udf_Time_KEY_NoHyphen(Start_Time)=dbo.udf_Time_KEY_NoHyphen(getdate())
---------------------------------------------------------------------------------------
Update 
	Report_Tables_Duration_Log 
SET
	Duration_time=
(
CASE
	WHEN
		[dbo].[udfTimeInterval](CAST(ISNULL((DATEDIFF(second,Start_Time,End_Time)),0)as bigint)) < '00:00:01'
	THEN
		'00:00:02'
	ELSE	
		[dbo].[udfTimeInterval](CAST(ISNULL((DATEDIFF(second,Start_Time,End_Time)),0)as bigint))
END
)
WHERE 
	[Procedure]='udsp_etl_Cloud_Partner_PNR_Rolling_13_Month_Delta_Agg' AND dbo.udf_Time_KEY_NoHyphen(Start_Time)=dbo.udf_Time_KEY_NoHyphen(getdate())
END	
----------------------------------------------------------------------------------------
--load Cloud_ENT_IB_PNR_Rep_Portfolio_Delta_Agg
----------------------------------------------------------------------------------------
IF 
	ISNULL((SELECT MAX(Time_Key) FROM Report_Tables_Duration_Log WHERE [Procedure]='udsp_etl_Cloud_ENT_IB_PNR_Rep_Portfolio_Delta_Agg'),0)  <> dbo.udf_Time_KEY_NoHyphen(getdate())
BEGIN
INSERT INTO 
	 Report_Tables_Duration_Log  
SELECT
	[Table]='Cloud_ENT_IB_PNR_Rep_Portfolio_Delta_Agg',
	[Procedure]='udsp_etl_Cloud_ENT_IB_PNR_Rep_Portfolio_Delta_Agg',
	Time_Key=dbo.udf_Time_KEY_NoHyphen(getdate()),
	Start_Time= Getdate(),
	End_Time='1/1/1900',
	Duration_time='1',
	[GROUP]='Cloud Comp Delta Load'
---------------------------------------------------------------------------------------
--EXEC EXEC_udsp_etl_Cloud_ENT_IB_PNR_Rep_Portfolio_Delta_Agg
---------------------------------------------------------------------------------------
Update 
	dbo.Report_Tables_Duration_Log 
SET 
	End_Time= Getdate()
WHERE 
	[Procedure]='udsp_etl_Cloud_ENT_IB_PNR_Rep_Portfolio_Delta_Agg' AND dbo.udf_Time_KEY_NoHyphen(Start_Time)=dbo.udf_Time_KEY_NoHyphen(getdate())
---------------------------------------------------------------------------------------
Update 
	Report_Tables_Duration_Log 
SET
	Duration_time=
(
CASE
	WHEN
		[dbo].[udfTimeInterval](CAST(ISNULL((DATEDIFF(second,Start_Time,End_Time)),0)as bigint)) < '00:00:01'
	THEN
		'00:00:02'
	ELSE	
		[dbo].[udfTimeInterval](CAST(ISNULL((DATEDIFF(second,Start_Time,End_Time)),0)as bigint))
END
)
WHERE 
	[Procedure]='udsp_etl_Cloud_ENT_IB_PNR_Rep_Portfolio_Delta_Agg' AND dbo.udf_Time_KEY_NoHyphen(Start_Time)=dbo.udf_Time_KEY_NoHyphen(getdate())
END	
----------------------------------------------------------------------------------------
--load udsp_etl_Cloud_Partner_PNR_Rep_Portfolio_Delta_Agg
----------------------------------------------------------------------------------------
IF 
	ISNULL((SELECT MAX(Time_Key) FROM Report_Tables_Duration_Log WHERE [Procedure]='udsp_etl_Cloud_Partner_PNR_Rep_Portfolio_Delta_Agg'),0)  <> dbo.udf_Time_KEY_NoHyphen(getdate())
BEGIN
INSERT INTO 
	 Report_Tables_Duration_Log  
SELECT
	[Table]='Cloud_Partner_PNR_Rep_Portfolio_Delta_Agg',
	[Procedure]='udsp_etl_Cloud_Partner_PNR_Rep_Portfolio_Delta_Agg',
	Time_Key=dbo.udf_Time_KEY_NoHyphen(getdate()),
	Start_Time= Getdate(),
	End_Time='1/1/1900',
	Duration_time='1',
	[GROUP]='Cloud Comp Delta Load'
---------------------------------------------------------------------------------------
--EXEC EXEC_udsp_etl_Cloud_Partner_PNR_Rep_Portfolio_Delta_Agg
---------------------------------------------------------------------------------------
Update 
	dbo.Report_Tables_Duration_Log 
SET 
	End_Time= Getdate()
WHERE 
	[Procedure]='udsp_etl_Cloud_Partner_PNR_Rep_Portfolio_Delta_Agg' AND dbo.udf_Time_KEY_NoHyphen(Start_Time)=dbo.udf_Time_KEY_NoHyphen(getdate())
---------------------------------------------------------------------------------------
Update 
	Report_Tables_Duration_Log 
SET
	Duration_time=
(
CASE
	WHEN
		[dbo].[udfTimeInterval](CAST(ISNULL((DATEDIFF(second,Start_Time,End_Time)),0)as bigint)) < '00:00:01'
	THEN
		'00:00:02'
	ELSE	
		[dbo].[udfTimeInterval](CAST(ISNULL((DATEDIFF(second,Start_Time,End_Time)),0)as bigint))
END
)
WHERE 
	[Procedure]='udsp_etl_Cloud_Partner_PNR_Rep_Portfolio_Delta_Agg' AND dbo.udf_Time_KEY_NoHyphen(Start_Time)=dbo.udf_Time_KEY_NoHyphen(getdate())
END	
----------------------------------------------------------------------------------------
--load Cloud_Ent_IB_PNR_Delta_Snapshot
----------------------------------------------------------------------------------------
IF 
	ISNULL((SELECT MAX(Time_Key) FROM Report_Tables_Duration_Log WHERE [Procedure]='udsp_etl_Cloud_ENT_IB_PNR_Delta_Snapshot'),0)  <> dbo.udf_Time_KEY_NoHyphen(getdate())
BEGIN
INSERT INTO 
	 Report_Tables_Duration_Log  
SELECT
	[Table]='Cloud_Ent_IB_PNR_Delta_Snapshot',
	[Procedure]='udsp_etl_Cloud_ENT_IB_PNR_Delta_Snapshot',
	Time_Key=dbo.udf_Time_KEY_NoHyphen(getdate()),
	Start_Time= Getdate(),
	End_Time='1/1/1900',
	Duration_time='1',
	[GROUP]='Cloud Comp Delta Load'
---------------------------------------------------------------------------------------
--EXEC EXEC_udsp_etl_Cloud_ENT_IB_PNR_Delta_Snapshot
---------------------------------------------------------------------------------------
Update 
	dbo.Report_Tables_Duration_Log 
SET 
	End_Time= Getdate()
WHERE 
	[Procedure]='udsp_etl_Cloud_ENT_IB_PNR_Delta_Snapshot' AND dbo.udf_Time_KEY_NoHyphen(Start_Time)=dbo.udf_Time_KEY_NoHyphen(getdate())
---------------------------------------------------------------------------------------
Update 
	Report_Tables_Duration_Log 
SET
	Duration_time=
(
CASE
	WHEN
		[dbo].[udfTimeInterval](CAST(ISNULL((DATEDIFF(second,Start_Time,End_Time)),0)as bigint)) < '00:00:01'
	THEN
		'00:00:02'
	ELSE	
		[dbo].[udfTimeInterval](CAST(ISNULL((DATEDIFF(second,Start_Time,End_Time)),0)as bigint))
END
)
WHERE 
	[Procedure]='udsp_etl_Cloud_ENT_IB_PNR_Delta_Snapshot' AND dbo.udf_Time_KEY_NoHyphen(Start_Time)=dbo.udf_Time_KEY_NoHyphen(getdate())
END	
----------------------------------------------------------------------------------------
--load Cloud_Partner_PNR_Delta_Snapshot
----------------------------------------------------------------------------------------
IF 
	ISNULL((SELECT MAX(Time_Key) FROM Report_Tables_Duration_Log WHERE [Procedure]='udsp_etl_Cloud_Partner_PNR_Delta_Snapshot'),0)  <> dbo.udf_Time_KEY_NoHyphen(getdate())
BEGIN
INSERT INTO 
	 Report_Tables_Duration_Log  
SELECT
	[Table]='Cloud_Partner_PNR_Delta_Snapshot',
	[Procedure]='udsp_etl_Cloud_Partner_PNR_Delta_Snapshot',
	Time_Key=dbo.udf_Time_KEY_NoHyphen(getdate()),
	Start_Time= Getdate(),
	End_Time='1/1/1900',
	Duration_time='1',
	[GROUP]='Cloud Comp Delta Load'
---------------------------------------------------------------------------------------
--EXEC EXEC_udsp_etl_Cloud_Partner_PNR_Delta_Snapshot
---------------------------------------------------------------------------------------
Update 
	dbo.Report_Tables_Duration_Log 
SET 
	End_Time= Getdate()
WHERE 
	[Procedure]='udsp_etl_Cloud_Partner_PNR_Delta_Snapshot' AND dbo.udf_Time_KEY_NoHyphen(Start_Time)=dbo.udf_Time_KEY_NoHyphen(getdate())
---------------------------------------------------------------------------------------
Update 
	Report_Tables_Duration_Log 
SET
	Duration_time=
(
CASE
	WHEN
		[dbo].[udfTimeInterval](CAST(ISNULL((DATEDIFF(second,Start_Time,End_Time)),0)as bigint)) < '00:00:01'
	THEN
		'00:00:02'
	ELSE	
		[dbo].[udfTimeInterval](CAST(ISNULL((DATEDIFF(second,Start_Time,End_Time)),0)as bigint))
END
)
WHERE 
	[Procedure]='udsp_etl_Cloud_Partner_PNR_Delta_Snapshot' AND dbo.udf_Time_KEY_NoHyphen(Start_Time)=dbo.udf_Time_KEY_NoHyphen(getdate())
END	
---------------------------------------------------------------------------------------
--udsp_etl_Bill_File_Invoice_Detail
---------------------------------------------------------------------------------------
IF 
	ISNULL((SELECT MAX(Time_Key) FROM Report_Tables_Duration_Log WHERE [Procedure]='udsp_etl_Bill_File_Invoice_Detail'),0)  <> dbo.udf_Time_KEY_NoHyphen(getdate())
BEGIN
INSERT INTO 
	Report_Tables_Duration_Log
SELECT
	[Table]='Bill_File_Invoice_Detail',
	[Procedure]='udsp_etl_Bill_File_Invoice_Detail',
	Time_Key=dbo.udf_Time_KEY_NoHyphen(getdate()),
	Start_Time= Getdate(),
	End_Time='1/1/1900',
	Duration_time='1',
	[GROUP]='Invoice Reporting'
---------------------------------------------------------------------------------------
--EXEC udsp_etl_Bill_File_Invoice_Detail
---------------------------------------------------------------------------------------
Update 
	dbo.Report_Tables_Duration_Log 
SET 
	End_Time= Getdate()
WHERE 
	[Procedure]='udsp_etl_Bill_File_Invoice_Detail' AND dbo.udf_Time_KEY_NoHyphen(Start_Time)=dbo.udf_Time_KEY_NoHyphen(getdate())
---------------------------------------------------------------------------------------
Update 
	Report_Tables_Duration_Log 
SET
	Duration_time=
(
CASE
	WHEN
		[dbo].[udfTimeInterval](CAST(ISNULL((DATEDIFF(second,Start_Time,End_Time)),0)as bigint)) < '00:00:01'
	THEN
		'00:00:02'
	ELSE	
		[dbo].[udfTimeInterval](CAST(ISNULL((DATEDIFF(second,Start_Time,End_Time)),0)as bigint))
END
)
WHERE 
	[Procedure]='udsp_etl_Bill_File_Invoice_Detail' AND dbo.udf_Time_KEY_NoHyphen(Start_Time)=dbo.udf_Time_KEY_NoHyphen(getdate())
END	
---------------------------------------------------------------------------------------
--udsp_Populate_Enterprise_Product_Level_Invoiced_Activated
---------------------------------------------------------------------------------------
IF 
	ISNULL((SELECT MAX(Time_Key) FROM Report_Tables_Duration_Log WHERE [Procedure]='udsp_Populate_Enterprise_Product_Level_Invoiced_Activated'),0)  <> dbo.udf_Time_KEY_NoHyphen(getdate())
BEGIN
INSERT INTO 
	Report_Tables_Duration_Log
SELECT
	[Table]='Enterprise_Product_Level_Invoiced_Activated',
	[Procedure]='udsp_Populate_Enterprise_Product_Level_Invoiced_Activated',
	Time_Key=dbo.udf_Time_KEY_NoHyphen(getdate()),
	Start_Time= Getdate(),
	End_Time='1/1/1900',
	Duration_time='1',
	[GROUP]='Invoice Reporting'
---------------------------------------------------------------------------------------
--EXEC udsp_Populate_Enterprise_Product_Level_Invoiced_Activated
---------------------------------------------------------------------------------------
Update 
	dbo.Report_Tables_Duration_Log 
SET 
	End_Time= Getdate()
WHERE 
	[Procedure]='udsp_Populate_Enterprise_Product_Level_Invoiced_Activated' AND dbo.udf_Time_KEY_NoHyphen(Start_Time)=dbo.udf_Time_KEY_NoHyphen(getdate())
---------------------------------------------------------------------------------------
Update 
	Report_Tables_Duration_Log 
SET
	Duration_time=
(
CASE
	WHEN
		[dbo].[udfTimeInterval](CAST(ISNULL((DATEDIFF(second,Start_Time,End_Time)),0)as bigint)) < '00:00:01'
	THEN
		'00:00:02'
	ELSE	
		[dbo].[udfTimeInterval](CAST(ISNULL((DATEDIFF(second,Start_Time,End_Time)),0)as bigint))
END
)
WHERE 
	[Procedure]='udsp_Populate_Enterprise_Product_Level_Invoiced_Activated' AND dbo.udf_Time_KEY_NoHyphen(Start_Time)=dbo.udf_Time_KEY_NoHyphen(getdate())
END	
---------------------------------------------------------------------------------------
GO
