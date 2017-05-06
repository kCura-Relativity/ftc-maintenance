/*****	Script Name : Full-text Catalog Maintenance Procedure v3.6

Author			:       Jared Lander
Versions		:       Tested on SQL 2008 R2 and SQL 2012
Description		:       Use this procedure to perform maintenance on full-text catalogs to defragment them.  The 
						procedure will check the number of fragments in the full-text catalogs for each database 
						on a server.  It accepts the following inputs:
						
						@reorgThreshold - any databases with less fragments than @reorgThreshold will be skipped, 
						any with >= @reorgThreshold and < @rebuildThrehold will be reorganized

						@rebuildThreshold - any databases with >= @rebuildThreshold will be rebuilt
						
						@stopAfter - this is the number of databases to perform maintenance on in one procedure 
						run; @windowLength will take precedence over @stopAfter if the estimated completion 
						time exceeds the end of the scheduled maintenance window
						
						@windowLength - number of minutes in the maintenance window; this is in order to try to 
						avoid exceeding the allotted time for maintenance to be performed (if @windowLength is 
						not specified and is left at the default of 0, the @stopAfter value alone will be used to 
						determine the end of the run)
						
						@monthsForAvg - number of months of history to analyze in order to estimate the time that 
						maintenance will take for each database.  A log is stored in the dbo.kIE_FTMaintenanceLog 
						table of the EDDSResource database so that average run times for the maintenance of each 
						database can be calculated.

						@maxSizeFTCinGB - if you schedule this as a regular maintenance plan, you may not want it 
						executing against a VERY large fulltext catalog.  Rebuilds can take a very long time on 
						very large workspaces, and keyword search results can be incomplete while a build is in 
						progress.  If needed, use this variable and handle large databases outside of the 
						regular maintenance plan.  The default of 0 means unlimited, thus it will not skip any 
						databases due to the size of the fulltext catalog.

Cautions		:       Please be aware that any failed or incomplete maintenance
						on a full-text catalog can cause keyword search to become
						unavailable or return inaccurate/incomplete results.

*******************************************************************************/

--Sample Procedure Run
/*
EXEC dbo.kIE_FTMaintenance
@reorgThreshold = 10,		--how many fragments before we take action?  default = 10
@rebuildThreshold = 30,		--how many fragments before we choose a rebuild over a reorganize?  default = 30
@stopAfter = 3,				--how many databases to reorganize or rebuild in one procedure run?  default = 3
@windowLength = 480,		--how many minutes in your maintenance window for this procedure?  if set to 0, we will rely solely on the @stopAfter parameter.  default = 0 (not set)
@monthsForAvg = 2			--when estimating whether or not a task will exceed the maintenance window, this is how many months of history will be viewed to estimate runtime.  default = 2
@maxSizeFTCinGB = 0			--if you schedule this as a regular maintenance plan, you may not want it executing against a VERY large fulltext catalog.  set the max here.  default = 0 (unlimited)
*/

USE master  --choose a different database here if you don't want the procedure to live in master
GO

IF EXISTS (SELECT 1 FROM sysobjects WHERE [Name] = 'kIE_FTMaintenance' AND [Type] = 'P')
BEGIN
	DROP PROCEDURE dbo.kIE_FTMaintenance
END
GO

CREATE PROCEDURE dbo.kIE_FTMaintenance
@reorgThreshold INT = 10,	
@rebuildThreshold INT = 30,	
@stopAfter INT = 3,			
@windowLength INT = 0,	
@monthsForAvg INT = 2,
@maxSizeFTCinGB INT = 0		
AS
BEGIN

DECLARE @SQL nvarchar(max)
DECLARE @x INT
DECLARE @i INT = 1
DECLARE @iMax INT
DECLARE @databaseName nvarchar(50)
DECLARE @procStart DATETIME = GETDATE()
DECLARE @procWindowEnd DATETIME
DECLARE @estimatedMinutes INT
DECLARE @beginTime DATETIME
DECLARE @endTime DATETIME

SET NOCOUNT ON

IF @windowLength <> 0
	SET @procWindowEnd = DATEADD(MINUTE, @windowLength, @procStart)

--Create work table with database names and fragment counts
CREATE TABLE #workTable(
ID INT IDENTITY (1,1) PRIMARY KEY,
Database_Name   SYSNAME,
FT_Fragment_Count INT 
)

--second work table to replicate the first, but in descending order of fragment count
CREATE TABLE #workTable2(
ID INT IDENTITY (1,1) PRIMARY KEY,
Database_Name SYSNAME,
FT_Fragment_Count INT
)

--if it doesn't already exist, create log table to store a history of start and end times for FT maintenance per database
IF NOT EXISTS(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'kIE_FTMaintenanceLog' and TABLE_SCHEMA = 'DBO')
BEGIN
	CREATE TABLE dbo.kIE_FTMaintenanceLog
	(
			FTLogID INT IDENTITY (1,1) PRIMARY KEY (FTLogID)
			,DatabaseName varchar(50)
			,ReorgOrRebuild varchar(10)
			,StartTime DATETIME
			,FinishTime DATETIME
			,DurationInMinutes INT
	)
END

--add databases with full-text catalogs to the work table
INSERT INTO #workTable (Database_Name)
SELECT name from sys.databases WITH (NOLOCK) WHERE name LIKE 'EDDS%' AND name NOT IN ('EDDS', 'EDDSPerformance', 'EDDSResource', 'EDDS1014823', 'EDDS1015024') --add additional databases to ignore here if desired
					
SELECT @iMax = MAX(ID) FROM #workTable

--loop to set fragment counts and delete databases from work table that do not contain a FTC for Relativity
WHILE @i <= @iMax
BEGIN
	SET @databaseName = (SELECT Database_Name FROM #workTable WHERE ID = @i)
	SET @SQL = 'SELECT @fragCount = COUNT(fragment_id) FROM @databaseName.sys.fulltext_index_fragments WITH (NOLOCK)'
	SET @SQL = REPLACE(@SQL, '@databaseName', @databaseName)
	EXECUTE sp_executesql @SQL, N'@fragCount INT OUTPUT', @fragCount = @x OUTPUT

	UPDATE #workTable
	SET FT_Fragment_Count = @x
	WHERE ID = @i

	--Delete the database from the work table if it does not contain a full text catalog for Relativity
	SET @SQL = 'IF NOT EXISTS (SELECT 1 FROM [@databaseName].[sys].[fulltext_catalogs] WHERE name = ''@databaseName'')
					DELETE FROM #workTable WHERE Database_Name = ''@databaseName'''
	SET @SQL = REPLACE(@SQL, '@databaseName', @databaseName)
	EXECUTE sp_executesql @SQL
	SET @i = @i + 1
END
--delete databases from work table that do not meet the fragmentation threshold
DELETE FROM #workTable WHERE FT_Fragment_Count < @reorgThreshold

--delete databases with a fulltext catalog larger than the threshold set by the @maxSizeFTCinGB value
IF @maxSizeFTCinGB <> 0
	DELETE FROM #workTable WHERE Database_Name IN (SELECT DB_NAME(database_id) FROM sys.master_files WHERE (size * 8.0 / 1024 / 1024) > @maxSizeFTCinGB and name LIKE 'ftrow_%')

--reset @i to be used on the new work table adjusted for databases with no FTC to work on
SET @i = 1
--reset @iMax to be the @stopAfter value so that we target the right number of databases
SET @iMax = @stopAfter

INSERT INTO #workTable2
SELECT Database_Name, FT_Fragment_Count FROM #workTable ORDER BY FT_Fragment_Count DESC
					
--delete any data from the log table that is older than 1 year
DELETE FROM dbo.kIE_FTMaintenanceLog WHERE FinishTime < DATEADD(YEAR, -1, GETDATE())

--Loop through the work table to perform maintenance
WHILE @i <= @iMax AND EXISTS (SELECT 1 FROM #workTable2 WHERE ID = @i)
BEGIN
	SET @databaseName = (SELECT Database_Name FROM #workTable2 WHERE ID = @i)
	--this section for a reorganize
	IF (SELECT FT_Fragment_Count FROM #workTable2 WHERE Database_Name = @databaseName) < @rebuildThreshold
	BEGIN
		SET @estimatedMinutes =  (SELECT COALESCE(AVG(DurationInMinutes),0) FROM dbo.kIE_FTMaintenanceLog WHERE DatabaseName = @databaseName AND ReorgOrRebuild = 'Reorganize' AND FinishTime > DATEADD(MONTH, (@monthsForAvg * -1), GETDATE()))
		IF NOT EXISTS (SELECT 1 FROM dbo.kIE_FTMaintenanceLog WHERE DatabaseName = @databaseName AND ReorgOrRebuild = 'Reorganize' AND FinishTime > DATEADD(MONTH, (@monthsForAvg * -1), GETDATE()))
			PRINT 'There is no runtime history for reorganizing the FTC on ' + @databaseName + ' in the past ' + CONVERT(varchar, @monthsForAvg) + ' months, but maintenance will proceed.  Runtime for this maintenance will be recorded for use in the future.'
		--if a window is given (not 0) and the estimated runtime for this database will not exceed the window, proceed with this database
		IF @windowLength <> 0 AND DATEADD(MINUTE, @estimatedMinutes, GETDATE()) < @procWindowEnd
		BEGIN
			SET @SQL = 'USE [@databaseName];
						ALTER FULLTEXT CATALOG [@databaseName] REORGANIZE;'
			SET @SQL = REPLACE(@SQL, '@databaseName', @databaseName)
			PRINT 'Full-text catalog reorganize started for database ' + @databaseName + ' on ' + CONVERT(varchar, GETDATE())
			SELECT @beginTime = GETDATE()
			BEGIN TRY
				EXECUTE sp_executesql @SQL
				SELECT @endTime = GETDATE()
				PRINT 'Full-text catalog reorganize completed for database ' + @databaseName + ' on ' + CONVERT(varchar, GETDATE())
				PRINT 'Time to reorganize full-text catalog for database ' + @databaseName + ':  ' + CONVERT(varchar, DATEDIFF(mi, @beginTime, @endTime)) + ' minutes.'
							
				INSERT INTO dbo.kIE_FTMaintenanceLog
				SELECT @databaseName, 'Reorganize', @beginTime, @endTime, DATEDIFF(mi, @beginTime, @endTime)
			END TRY
			BEGIN CATCH
				PRINT 'Reorganization of catalog ' + @databaseName + ' failed with the following error:  ' + ERROR_MESSAGE()
			END CATCH
		END
		--if a window is given and the estimated runtime for this database will exceed the window, skip to the next database
		ELSE IF @windowLength <> 0 AND DATEADD(MINUTE, @estimatedMinutes, GETDATE()) > @procWindowEnd
		BEGIN
			PRINT 'The maintenance for database ' + @databaseName + ' is likely to exceed the allotted maintenance window.  Skipping this database.'
			SET @iMax = @iMax + 1 --Since this database is being skipped, add 1 to @iMax so we still target the right number of databases
		END
		--if a window is not given (ie. @windowLength is set to 0)
		ELSE
		BEGIN
			SET @SQL = 'USE [@databaseName];
			ALTER FULLTEXT CATALOG [@databaseName] REORGANIZE;'
			SET @SQL = REPLACE(@SQL, '@databaseName', @databaseName)
			PRINT 'Full-text catalog reorganize started for database ' + @databaseName + ' on ' + CONVERT(varchar, GETDATE())
			SELECT @beginTime = GETDATE()
			BEGIN TRY
				EXECUTE sp_executesql @SQL
				SELECT @endTime = GETDATE()
				PRINT 'Full-text catalog reorganize completed for database ' + @databaseName + ' on ' + CONVERT(varchar, GETDATE())
				PRINT 'Time to reorganize full-text catalog for database ' + @databaseName + ':  ' + CONVERT(varchar, DATEDIFF(mi, @beginTime, @endTime)) + ' minutes.'
							
				INSERT INTO dbo.kIE_FTMaintenanceLog
				SELECT @databaseName, 'Reorganize', @beginTime, @endTime, DATEDIFF(mi, @beginTime, @endTime)
			END TRY
			BEGIN CATCH
				PRINT 'Reorganization of catalog ' + @databaseName + ' failed with the following error:  ' + ERROR_MESSAGE()
			END CATCH
		END
	END
	--this section for rebuilds
	ELSE IF (SELECT FT_Fragment_Count FROM #workTable WHERE Database_Name = @databaseName) >= @rebuildThreshold
	BEGIN
		SET @estimatedMinutes =  (SELECT COALESCE(AVG(DurationInMinutes),0) FROM dbo.kIE_FTMaintenanceLog WHERE DatabaseName = @databaseName AND ReorgOrRebuild = 'Rebuild' AND FinishTime > DATEADD(MONTH, (@monthsForAvg * -1), GETDATE()))
		IF NOT EXISTS (SELECT 1 FROM dbo.kIE_FTMaintenanceLog WHERE DatabaseName = @databaseName AND ReorgOrRebuild = 'Rebuild' AND FinishTime > DATEADD(MONTH, (@monthsForAvg * -1), GETDATE()))
			PRINT 'There is no runtime history for rebuilding the FTC on ' + @databaseName + ' in the past ' + CONVERT(varchar, @monthsForAvg) + ' months, but maintenance will proceed.  Runtime for this maintenance will be recorded for use in the future.'

		--if a window is given (not 0) and the estimated runtime for this database will not exceed the window, proceed with this database
		IF @windowLength <> 0 AND DATEADD(MINUTE, @estimatedMinutes, GETDATE()) < @procWindowEnd
		BEGIN
			SET @SQL = 'USE [@databaseName];
						ALTER FULLTEXT CATALOG [@databaseName] REBUILD;'
			SET @SQL = REPLACE(@SQL, '@databaseName', @databaseName)
			PRINT 'Full-text catalog rebuild started for database ' + @databaseName + ' on ' + CONVERT(varchar, GETDATE())
			SELECT @beginTime = GETDATE()
			BEGIN TRY
				EXECUTE sp_executesql @SQL
				--check the dmv until we no longer see the full population in progress; that's how we'll know when it has finished
				WHILE EXISTS (SELECT 1 FROM sys.dm_fts_index_population WHERE database_id = DB_ID(@databaseName) AND population_type = 1)
				BEGIN
					WAITFOR DELAY '00:00:30'
					IF GETDATE() > @procWindowEnd
					BEGIN	
						IF @SQL <> 'X'
						BEGIN
							PRINT 'The maintenance window has been exceeded, but a full-text catalog rebuild is still in progress on database ' + @databaseName + '.  Full-text (keyword) searching against this database will not be accurate until the rebuild operation completes.' 
							SET @SQL = 'X'
						END
					END
				END
				SELECT @endTime = GETDATE()
				PRINT 'Full-text catalog rebuild completed for database ' + @databaseName + ' on ' + CONVERT(varchar, GETDATE())
				PRINT 'Time to rebuild full-text catalog for database ' + @databaseName + ':  ' + CONVERT(varchar, DATEDIFF(mi, @beginTime, @endTime)) + ' minutes.'
							
				INSERT INTO dbo.kIE_FTMaintenanceLog
				SELECT @databaseName, 'Rebuild', @beginTime, @endTime, DATEDIFF(mi, @beginTime, @endTime)
			END TRY
			BEGIN CATCH
				PRINT 'Rebuild of catalog ' + @databaseName + ' failed with the following error:  ' + ERROR_MESSAGE()
			END CATCH
		END
		--if a window is given and the estimated runtime for this database will exceed the window, skip to the next database
		ELSE IF @windowLength <> 0 AND DATEADD(MINUTE, @estimatedMinutes, GETDATE()) > @procWindowEnd
		BEGIN
			PRINT 'The maintenance for database ' + @databaseName + ' is likely to exceed the allotted maintenance window.  Skipping this database.'
			SET @iMax = @iMax + 1 --Since this database is being skipped, add 1 to @iMax so we still target the right number of databases
		END
		--if a window is not given (ie. @windowLength is set to 0)
		ELSE
		BEGIN
			SET @SQL = 'USE [@databaseName];
			ALTER FULLTEXT CATALOG [@databaseName] REBUILD;'
			SET @SQL = REPLACE(@SQL, '@databaseName', @databaseName)
			PRINT 'Full-text catalog rebuild started for database ' + @databaseName + ' on ' + CONVERT(varchar, GETDATE())
			SELECT @beginTime = GETDATE()
			BEGIN TRY
				EXECUTE sp_executesql @SQL
				--check the dmv until we no longer see the full population in progress; that's how we'll know when it has finished
				WHILE EXISTS (SELECT 1 FROM sys.dm_fts_index_population WHERE database_id = DB_ID(@databaseName) AND population_type = 1)
				BEGIN
					WAITFOR DELAY '00:00:30'
					IF GETDATE() > @procWindowEnd
					BEGIN	
						IF @SQL <> 'X'
						BEGIN
							PRINT 'The maintenance window has been exceeded, but a full-text catalog rebuild is still in progress on database ' + @databaseName + '.  Full-text (keyword) searching against this database will not be accurate until the rebuild operation completes.' 
							SET @SQL = 'X'
						END
					END

				END
				SELECT @endTime = GETDATE()
				PRINT 'Full-text catalog rebuild completed for database ' + @databaseName + ' on ' + CONVERT(varchar, GETDATE())
				PRINT 'Time to rebuild full-text catalog for database ' + @databaseName + ':  ' + CONVERT(varchar, DATEDIFF(mi, @beginTime, @endTime)) + ' minutes.'
							
				INSERT INTO dbo.kIE_FTMaintenanceLog
				SELECT @databaseName, 'Rebuild', @beginTime, @endTime, DATEDIFF(mi, @beginTime, @endTime)
			END TRY
			BEGIN CATCH
				PRINT 'Rebuild of catalog ' + @databaseName + ' failed with the following error:  ' + ERROR_MESSAGE()
			END CATCH
		END
	END

	SET @i = @i + 1
	--if the maintenance window has passed, set @i greater than @iMax to exit the loop
	IF GETDATE() > @procWindowEnd
	BEGIN
		SET @i = @iMax + 1
		PRINT 'The maintenance window has been exceeded.  No more work will be done in this run.'
	END
END
PRINT 'Maintenance Procedure Complete.'

DROP TABLE #workTable;
DROP TABLE #workTable2;

END