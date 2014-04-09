
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[usp_updateFromLoadTables') AND type in (N'P'))
  drop procedure usp_updateFromLoadTables
go


--
--  procedure to perform sanity checks on the raw data tables loaded externally
--  and then load them into properly keyed and indexed tables
--  load happens per top level data row such that only offending data is uniformly excluded across all pertinent tables
--
--  provides an point of failure that will not affect existing snapshot data
--
CREATE PROCEDURE [dbo].[usp_updateFromLoadTables]
(
  @errorText varchar(max) OUTPUT,
  @version decimal(4,1) = NULL
)
AS
BEGIN
  SET NOCOUNT ON;
  --ERROR HANDLING
  DECLARE @SPID int
  DECLARE @SERVERID nvarchar(128)
  DECLARE @DBNAME VARCHAR(128)
  declare @table varchar(100), @client varchar(100), @sql varchar(max), @srcTbl varchar(100),@concat varchar(max)
  declare @tblDeals table (deal varchar(8), rowNum int primary key);
  declare @curDeal varchar(8)
  declare @dealNum int
  declare @tblErrors table ( msg varchar(max) )
  declare @tblTables table (rowNum int identity primary key, [table] varchar(80));
  declare @curTable varchar(80)
  declare @tableNum int

  declare @key varchar(14)
  declare @eBuf varchar(max)
  DECLARE @errDesc varchar(max)


  SET @DBNAME = DB_NAME()
  SET @SPID = @@SPID
  SET @SERVERID = @@servername


  if (@version is NULL)
    set @version = 11.0

  --  how to avoid cursors and all their evil
  --  i.e. set up a sequentially indexed table with foreign key values for a while loop
  --
  insert into @tblDeals
  select d.DealName, ROW_NUMBER() OVER (ORDER BY d.DealName desc) AS [rowNum]
  from dfDealLoad d


  --  list of tables for this version
  --
  insert into @tblTables
  select distinct([table]) from fileFieldMap where [version] = @version and [table] not like '%Load'


    ----------------------------------------
  -------   Per  row Outer loop   ------------
    ----------------------------------------

  select @dealNum = max(rowNum) from @tblDeals
  while (@dealNum > 0)
  begin
    select @curDeal = deal from @tblDeals where rowNum = @dealNum
    --print 'curDeal='+@curDeal

    BEGIN TRY

      set @eBuf = NULL

      ------------------------------------------------------------------
      -----  ensure distdates are the same per deal across all tables  -----
      ------------------------------------------------------------------
      create table #tblDD ( distdate int )

      insert into #tblDD
      select TapeDate as distdate
      from dfBondLoad
      where TreppDealName = @curDeal

      insert into #tblDD
      select distdate
      from dfLoanLoad
      where dosname = @curDeal

      insert into #tblDD
      select distdate
      from dfLoan2Load
      where dosname = @curDeal

      insert into #tblDD
      select distdate
      from dfNoteLoad
      where dosname = @curDeal

      insert into #tblDD
      select distdate
      from dfPropLoad
      where dosname = @curDeal

      declare @distdates varchar(max)
      set @distdates = NULL;
      select @distdates = coalesce(@distdates+', ','') + cast(distdate as varchar(8))
      from #tblDD
      group by distdate

      drop table #tblDD

      --------------------------------------------
      --------  otherwise, hit the catch block  ------
      --------------------------------------------
      if (len(@distdates) > 8)
      begin
        SET @eBuf = 'distdate mismatch for deal '+@curDeal + ', dates: ' + @distdates
        print @eBuf
        raiserror(@eBuf, 11, 0)
      end

      --  update all the tables for this deal
      --
      BEGIN TRAN;

        -------------------------------------
        --------  Per Table -  Inner loop  ------
        -------------------------------------

        select @tableNum = max(rowNum) from @tblTables

        while (@tableNum > 0)
        begin
          select @curTable = [table] from @tblTables where rowNum = @tableNum

          if (@curTable = 'dfBond')
            set @client = 'ratings'
          else
            set @client = 'default'

          ----------------------------------------------------------

          select @key = [key] from fileFieldMap where [table] = @curTable and client = @client and [version] = @version;
          select @srcTbl = @curTable + 'Load'

          --print 'curTable: '+@curTable + ' srcTbl: '+@srcTbl+ ' client: '+@client;

          set @concat = NULL;
          select @concat = coalesce(@concat+', ','' ) + '[' + COLUMN_NAME + ']' from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = @curTable

          set @sql = 'DELETE from '+@curTable+' where '+@key+' = '+''''+@curDeal+''''
          --print @sql;
          exec(@sql)

          set @sql = 'INSERT into '+@curTable + ' ('+@concat+') SELECT ' + replace(@concat, '[', 't.[') + ' FROM ' + @srcTbl + ' t where t.'+@key+' = '+''''+@curDeal+''''
          --print @sql;
          exec(@sql)

          set @tableNum = @tableNum - 1
        end

      commit tran;

    END TRY
    BEGIN CATCH
      SET @errDesc = 'ERROR: [' + OBJECT_NAME(@@PROCID) + '] ' + ERROR_MESSAGE() + ' on line ' + convert(varchar,ERROR_LINE())
      IF @@TRANCOUNT > 0
      BEGIN
        set @errDesc = @errDesc + ', rolling back transaction'
        ROLLBACK TRANSACTION;
      END;

      if (@errDesc is not null)
      begin
        print @errDesc
        insert into @tblErrors (msg) values(@errDesc);
      end

    END CATCH;

    set @dealNum = @dealNum - 1
  end   --end while

  select @errorText = (case when msg is not null and msg != '' then coalesce(@errorText + char(10), '') + msg end) from @tblErrors
END

GO
