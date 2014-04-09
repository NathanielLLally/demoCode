-- ===================================================
-- Author:		<Nate Lally>
-- Create date: <2011-08>
-- Description:	populate dfDeal table with crosstabs
--   method is to pre-join static data
--   with required fields for calcs only
--  then we have our xml -> pivot function which generates dynamic sql
--    to perform table crosstabs (pivots)
--
--  example output is at the bottom
--  
-- ===================================================

create PROCEDURE [dbo].[usp_xferdfDeal]
AS
BEGIN
	SET NOCOUNT ON;
	--ERROR HANDLING
	DECLARE @SPID int
	DECLARE @SERVERID nvarchar(128)
	DECLARE @DBNAME VARCHAR(128)
	DECLARE @rows int
	declare @errorText varchar(1000)
	declare @e2 varchar(1000)
	
declare @type varchar(10), @field varchar(80), @name varchar(80), @lastName varchar(80), @values nvarchar(max), @datafeedFields nvarchar(max), @srcFields nvarchar(max)
declare @aggregateClause nvarchar(max), @sourceClause nvarchar(max),  @sql nvarchar(max), @id int, @postField varchar(160), @topx int

	
	SET @DBNAME = DB_NAME()
	SET @SPID = @@SPID  
	SET @SERVERID = @@servername
	--begin try
	--	begin
			--  truncated datafeeddb.dbo.dfDeal and pulls non xtab fields
			exec usp_upd_dfDeal @errorText = @e2 OUTPUT
	--	end
	--END TRY
	--BEGIN CATCH

	--	RAISERROR(@e2,11,0,@SERVERID,@DBNAME,@SPID)
	--END CATCH

	BEGIN TRY
		BEGIN TRAN
			begin


--   pre-joins dealRpt, loanRpt, propRpt, and bondRpt with the minimal fields needed for the all the following tabulations
--     with a few extras (ranking for top x stats)
--
--   this alone saves over 10 minutes
--
truncate table datafeeddb.dbo.dl1

insert into datafeeddb.dbo.dl1
select d.dealID, d.dealName, d.curDealBal, 
	l.loanID, l.poolnum, l.curLoanBal, l.curMasterServReturnDt, l.curSpecServTransDt, l.curDlqCode,
	dbo.noZeros(l.curCpn) as curCpn, dbo.noZeros(l.mrAser) as mrAser, dbo.noZeros(l.appRedAmtTrustee) as appRedAmtTrustee,
	l.secLoanBal, l.rateIndex, l.defeasStatus, l.propName, l.curMonLock, l.curMonYMC, l.curMonPP, 0 as lbalRank, 0 as defRank,
	le1.pmtFreq, convert(decimal(18,8), le1.balloonLTV) as balloonLTV,
	le2.watchListDt
--into datafeeddb.dbo.dl1
from treppwebdb_3.dbo.dealRpt d
inner join treppwebdb_3.dbo.loanRpt l
	on d.dealID = l.dealID
--		and 1.5 < d.curDealBal
	and 0.015 < l.curLoanBal
inner join treppwebdb_3.dbo.loanRpt_ext1 le1
	on l.dealID = le1.dealID
	and l.loanID = le1.loanID
inner join treppwebdb_3.dbo.loanRpt_ext2 le2
	on l.dealID = le2.dealID
	and l.loanID = le2.loanID
inner join treppwebdb_3.dbo.loadDb_dealsToMove ld on d.dealID = ld.dealID
	--where datediff(dd,ld.load_db_runDt, getdate()) < 1                     


-----------------------------------------------------------------------------------------

update datafeeddb.dbo.dl1 set lbalRank = sq.lbalRank
from datafeeddb.dbo.dl1 dl
join
(select dealName, curLoanBal, loanID,
	row_number() over(partition by dealName order by curLoanBal desc) as lbalRank
	from datafeeddb.dbo.dl1 l
	where l.curDealBal > 1
	group by dealName, curLoanBal, loanID) sq
on dl.dealName = sq.dealName and dl.loanID = sq.loanID

update datafeeddb.dbo.dl1 set defRank = sq.defRank
from datafeeddb.dbo.dl1 dl
join
(select dealName, curLoanBal, loanID,
	row_number() over(partition by dealName order by curLoanBal desc) as defRank
	from datafeeddb.dbo.dl1 l
	where l.curDealBal > 1
	and defeasStatus in ('F', '0', 'M', 'P')
	group by dealName, curLoanBal, loanID) sq
on dl.dealName = sq.dealName and dl.loanID = sq.loanID


---------
truncate table datafeeddb.dbo.dp1

insert into datafeeddb.dbo.dp1
select d.dealID, d.dealName,d.curDealBal,
	p.propID, p.loanID, p.curPropBal, p.propTypeCode, p.propTypeNorm, p.state
--into datafeeddb.dbo.dp1
from dealRpt d
inner join propRpt p
	on d.dealID = p.dealID
	and 0.015 < p.curPropBal
inner join loadDb_dealsToMove ld on d.dealID = ld.dealID
--where datediff(dd,ld.load_db_runDt, getdate()) < 1                     

------------
--drop table datafeeddb.dbo.db1
truncate table datafeeddb.dbo.db1

insert into datafeeddb.dbo.db1
select b.dealID, d.dealName, b.bondID, b.secCreditEnhance, b.curCreditEnhance, b.isIO,
	b.curRatingSnP, b.curRatingMoodys, b.curRatingFitch
--into datafeeddb.dbo.db1
from bondRpt b
join dealRpt d on b.dealID = d.dealID
inner join loadDb_dealsToMove ld on d.dealID = ld.dealID
--where datediff(dd,ld.load_db_runDt, getdate()) < 1                     

	
-------------------


-----------   agency subordination   --------------------
--drop table #agencies

select dealName, agency, rating, secCreditEnhance, curCreditEnhance
into #agencies
FROM
(
select dealName, secCreditEnhance, curCreditEnhance, isIO,
 upper(curRatingSnP) as [S&P], upper(curRatingMoodys) as [Moodys], upper(curRatingFitch) as Fitch
from datafeeddb.dbo.db1
) p
unpivot
	(rating FOR agency in
		([Moodys], [Fitch], [S&P])
) as unpvt
where isIO is null 

-----------
update datafeeddb.dbo.dfDeal set [CutoffAaaSubordination] = a.[CutoffAaaSubordination], [CurrentAaaSubordination] = a.[CurrentAaaSubordination]
from datafeeddb.dbo.dfDeal dfd join
(
select dealName, min(secCreditEnhance) as [CutoffAaaSubordination], min(curCreditEnhance) as [CurrentAaaSubordination]
FROM #agencies
where (rating = 'AAA')
group by dealName
) a on a.dealName = dfd.DealName

-----------------
update datafeeddb.dbo.dfDeal set [CutoffBaa3Subordination] = a.[CutoffBaa3Subordination], [CurrentBaa3Subordination] = a.[CurrentBaa3Subordination]
from datafeeddb.dbo.dfDeal dfd join
(
select dealName, min(secCreditEnhance) as [CutoffBaa3Subordination], min(curCreditEnhance) as [CurrentBaa3Subordination]
FROM #agencies
where (rating like 'A%' or rating like 'BBB%' or rating like 'BAA%')
group by dealName
) a on a.dealName = dfd.DealName

--------------------------
update datafeeddb.dbo.dfDeal set [RatingAgencies] = a.[RatingAgencies]
from datafeeddb.dbo.dfDeal dfd join
(
select dealName, replace(ltrim(coalesce([Moodys], '') + coalesce(' ' + [Fitch], '') + coalesce(' ' + [S&P], '')), ' ', '/') as RatingAgencies
from
(
select dealName, agency
FROM #agencies
where (rating like 'A%' or rating like 'BBB%' or rating like 'BAA%')
group by dealName, agency
) src
pivot
(
	min(agency)
	for agency in ([Moodys], [Fitch], [S&P])
) as p
) a on a.dealName = dfd.DealName

--
--  xml format for dynamic pivots
--  crosstab name: description, logical block
--    <field name="str" topx="n"> : field name base string [topx=n] optional valued replacing subsequent %x
--    <type name="str"> :  appends string, logical block
--      <post_field>: clause post source field
--      <aggregate>FUNC(sourceField></aggregate>: aggregate function for pivot value field
--      <source>sql</source>  select statement sourcing data for pivot (%f replaced by field, %x by topx value)
--
---------------------------------------------------------------------
-- common XML escapes
-- & (Ampersand)	&amp;
-- < (Left angle bracket)	&lt;
-- > (Right angle bracket)	&gt;
-- ” (Straight quotation mark)	&quot;
-- ‘ (Apostrophe)	&apos;

declare @xml xml, @idoc int
set @xml = '<root>
	<crosstab name="Top 3 defeased loans">
		<field name="Top1DefeasedLoan" topx="1"/>
		<field name="Top2DefeasedLoan" topx="2"/>
		<field name="Top3DefeasedLoan" topx="3"/>
		<type name="">
			<aggregate>
				MIN(propName)
			</aggregate>
			<source>
				select dealName, %field as value_field, propName from datafeeddb.dbo.dl1
				where curDealBal &gt; 1.5
				and defeasStatus in (''F'', ''0'', ''M'', ''P'')
				and defRank = %x
			</source>
		</type>
		<type name="Amt">
			<aggregate>
				MIN(curLoanBal)
			</aggregate>
			<source>
				select dealName, %field as value_field, curLoanBal from datafeeddb.dbo.dl1 dl1
				where curDealBal &gt; 1.5
				and defeasStatus in (''F'', ''0'', ''M'', ''P'')
				and defRank = %x
			</source>
		</type>
		<type name="Pct">
			<post_field>
				*100 / curDealBal
			</post_field>
			<aggregate>
				MIN(curLoanBal)
			</aggregate>
			<source>
				select dealName, %field as value_field, curLoanBal, curDealBal from datafeeddb.dbo.dl1 dl1
				where curDealBal &gt; 1.5
				and defeasStatus in (''F'', ''0'', ''M'', ''P'')
				and defRank = %x
			</source>
		</type>
		<type name="Status">
			<aggregate>
				MIN(defeasStatusDesc)
			</aggregate>
			<source>
				select dl.dealName, %field as value_field, dc.defeasStatusDesc from datafeeddb.dbo.dl1 dl
				inner join defeasanceCats dc
					on dl.defeasStatus = dc.defeasStatus
				where dl.curDealBal &gt; 1.5
				and dl.defeasStatus in (''F'', ''0'', ''M'', ''P'')
				and dl.defRank = %x
			</source>
		</type>

	</crosstab>
	
	<crosstab name="Loan Concentration Statistics">
		<field name="Top1Loans" topx="1" />
		<field name="Top5Loans" topx="5" />
		<field name="Top10Loans" topx="10" />
		<field name="Top15Loans" topx="15" />
		<type name="Amt">
			<aggregate>
				SUM(curLoanBal)
			</aggregate>
			<source>
	select dealName, curLoanBal, %field as value_field
	from datafeeddb.dbo.dl1
	where lbalRank &lt;= %x and curDealBal is not NULL
			</source>
		</type>
		<type name="Pct">
			<aggregate>
				SUM(curLoanBal)
			</aggregate>
			<post_field>
				*100 / curDealBal
			</post_field>
			<source>
	select dealName, curLoanBal, curDealBal, %field as value_field
	from datafeeddb.dbo.dl1
	where [lbalRank] &lt;= %x
			</source>
		</type>
	</crosstab>

	<crosstab name="Loan Restriction Statistics">
		<field name="Lock" />
		<field name="YM" />
		<field name="PP" />
		<field name="Open" />
		<type name="Pct">
			<post_field>
				*100/curDealBal
			</post_field>
			<aggregate>
				SUM(curLoanBal)
			</aggregate>
			<source>
				select dl1.dealName as dealName,
				case
					when 0 &lt; isnull(curMonLock, 0)	then ''Lock''
					when 0 &lt; isnull(curMonYMC, 0) then ''YM''
					when 0 &lt; isnull(curMonPP, 0) then ''PP''
					else ''Open''
			    end as value_field, dl1.curLoanBal, dl1.curDealBal
				from datafeeddb.dbo.dl1 dl1
				where curDealBal &gt; 1
			</source>
		</type>
	</crosstab>

	<crosstab name="Defease loan concentration">
		<field name="Defease" />
		<type name="Amt">
			<aggregate>
				SUM(curLoanBal)
			</aggregate>
			<source>
				select dealName, ''Defease'' as value_field, curLoanBal from datafeeddb.dbo.dl1 dl1
				where curDealBal &gt; 1
				and defeasStatus in (''F'', ''0'', ''M'', ''P'')
			</source>
		</type>
		<type name="Cnt">
			<aggregate>
				COUNT(loanID)
			</aggregate>
			<source>
				select dealName, ''Defease'' as value_field, loanID from datafeeddb.dbo.dl1 dl1
				where curDealBal &gt; 1
				and defeasStatus in (''F'', ''0'', ''M'', ''P'')
			</source>
		</type>
		<type name="Pct">
			<post_field>
				*100 / curDealBal
			</post_field>
			<aggregate>
				SUM(curLoanBal)
			</aggregate>
			<source>
				select dealName, ''Defease'' as value_field, curLoanBal, curDealBal from datafeeddb.dbo.dl1 dl1
				where curDealBal &gt; 1
				and defeasStatus in (''F'', ''0'', ''M'', ''P'')
			</source>
		</type>
	</crosstab>

	<crosstab name="Rate Type Statistics">
		<field name="Fixed" />
		<field name="Floating" />
		<type name="Amt">
			<aggregate>
				SUM(curLoanBal)
			</aggregate>
			<source>
				select dealName, case when rateIndex is null then ''Fixed'' else ''Floating'' end as value_field,
				curLoanBal from datafeeddb.dbo.dl1 dl1
				where curDealBal &gt; 1.5
			</source>
		</type>
		<type name="Cnt">
			<aggregate>
				COUNT(loanID)
			</aggregate>
			<source>
				select dealName, case when rateIndex is null then ''Fixed'' else ''Floating'' end as value_field,
				loanID from datafeeddb.dbo.dl1 dl1
				where curDealBal &gt; 1.5
			</source>
		</type>
		<type name="Pct">
			<post_field>
				*100 / curDealBal
			</post_field>
			<aggregate>
				SUM(curLoanBal)
			</aggregate>
			<source>
				select dealName, case when rateIndex is null then ''Fixed'' else ''Floating'' end as value_field,
				curLoanBal, curDealBal
				from datafeeddb.dbo.dl1 dl1
				where curDealBal &gt; 1.5
			</source>
		</type>
	</crosstab>
	
	<crosstab name="Watchlist">
		<field name="Watchlist" />
		<type name="Amt">
			<aggregate>
				SUM(curLoanBal)
			</aggregate>
			<source>
				select dealName, ''Watchlist'' as value_field, curLoanBal from datafeeddb.dbo.dl1 dl1
				where isnull(watchListDt, ''19000101'') &gt; ''19000101''
			</source>
		</type>
		<type name="Cnt">
			<aggregate>
				COUNT(loanID)
			</aggregate>
			<source>
				select dealName, ''Watchlist'' as value_field, loanID from datafeeddb.dbo.dl1 dl1
				where isnull(watchListDt, ''19000101'') &gt; ''19000101''
			</source>
		</type>
		<type name="Pct">
			<post_field>
				*100 / curDealBal
			</post_field>
			<aggregate>
				SUM(curLoanBal)
			</aggregate>
			<source>
				select dealName, ''Watchlist'' as value_field, curLoanBal, curDealBal from datafeeddb.dbo.dl1 dl1
				where isnull(watchListDt, ''19000101'') &gt; ''19000101''
			</source>
		</type>
	</crosstab>

	<crosstab name="Top States">
		<field name="State1" />
		<field name="State2" />
		<field name="State3" />
		<field name="State4" />
		<field name="State5" />
		<type name="Name">
			<aggregate>
				MIN(Name)
			</aggregate>
			<comment>
				take deal ''ballhltn'', should 60 percent of the deal not make the list in any fashion?
				case when p.state is null then 1 else 0 end
			</comment>
			<source>
			select p.dealName, p.state as Name,
				''State'' + convert(varchar(4), rank() over(partition by p.dealName order by case when p.state is null then 1 else 0 end, sum(curPropBal) desc)) as value_field
				from datafeeddb.dbo.dp1 p
				where p.curDealBal > 1.5
				group by p.dealName, p.curDealBal, p.state
			</source>
		</type>

		<type name="Amt">
			<aggregate>
				MIN([sum])
			</aggregate>
			<comment>
				take deal ''ballhltn'', should 60 percent of the deal not make the list in any fashion?
				case when p.state is null then 1 else 0 end
			</comment>
			<source>
				select p.dealName, p.state as Name, sum(p.curPropBal) as sum,
				''State'' + convert(varchar(4), rank() over(partition by p.dealName order by case when p.state is null then 1 else 0 end, sum(curPropBal) desc)) as value_field
				from datafeeddb.dbo.dp1 p
				where p.curDealBal > 1.5
				group by p.dealName, p.curDealBal, p.state
			</source>
		</type>
		<type name="PropCnt">
			<aggregate>
				MIN([cnt])
			</aggregate>
			<source>
				select p.dealName, count(p.propID) as cnt,
				''State'' + convert(varchar(4), rank() over(partition by p.dealName order by case when p.state is null then 1 else 0 end, sum(curPropBal) desc)) as value_field
				from datafeeddb.dbo.dp1 p
				where p.curDealBal > 1.5
				group by p.dealName, p.curDealBal, p.state
			</source>
		</type>
		<type name="Pct">
			<aggregate>
				min(pct)
			</aggregate>
			<post_field>
			</post_field>
			<source>
				select p.dealName, p.state as Name, sum(p.curPropBal) * 100 / p.curDealBal as pct,
				''State'' + convert(varchar(4), rank() over(partition by p.dealName order by case when p.state is null then 1 else 0 end, sum(curPropBal) desc)) as value_field
				from datafeeddb.dbo.dp1 p
				where p.curDealBal > 1.5
				group by p.dealName, p.curDealBal, p.state
			</source>
		</type>
	</crosstab>

	<crosstab name="Property Type Statistics">
		<field name="CTL" />
		<field name="Healthcare" />
		<field name="HotelFull" />
		<field name="HotelLimited" />
		<field name="HotelOther" />
		<field name="Industrial" />
		<field name="MixedUse" />
		<field name="MobileHome" />
		<field name="Multifamily" />
		<field name="Office" />
		<field name="Other" />
		<field name="RetailAnchored" />
		<field name="RetailUnanchored" />
		<field name="SelfStorage" />
		<field name="Warehouse" />
		<field name="TypeUndefined" />
		<type name="Amt">
			<aggregate>
				SUM(curPropBal)
			</aggregate>
			<source>
				select dealName, isnull(f.propTypeFeed, ''TypeUndefined'') as value_field, curPropBal
					from datafeeddb.dbo.dp1 dp1
					left outer join datafeeddb.dbo.feed_proptype f
						on dp1.propTypeNorm = f.propTypeNorm
					where dp1.curDealBal &gt; 1
			</source>
		</type>
		<type name="Cnt">
			<aggregate>
				COUNT(loanID)
			</aggregate>
			<source>
				select dealName, isnull(f.propTypeFeed, ''TypeUndefined'') as value_field, loanID
					from datafeeddb.dbo.dp1 dp1
					left outer join datafeeddb.dbo.feed_proptype f
						on dp1.propTypeNorm = f.propTypeNorm
					where dp1.curDealBal &gt; 1
			</source>
		</type>
		<type name="Pct">
			<aggregate>
				sum(curPropBal)
			</aggregate>
			<post_field>
				*100/curDealBal
			</post_field>
			<source>
				select dealName, isnull(f.propTypeFeed, ''TypeUndefined'') as value_field, curPropBal, curDealBal
					from datafeeddb.dbo.dp1 dp1
					left outer join datafeeddb.dbo.feed_proptype f
						on dp1.propTypeNorm = f.propTypeNorm
					where dp1.curDealBal &gt; 1
			</source>
		</type>
	</crosstab>

	<crosstab name="Coop Housing">
		<field name="CoopHousing" />
		<type name="Amt">
			<aggregate>
				SUM(curPropBal)
			</aggregate>
			<source>
				select dealName, ''CoopHousing'' as value_field, curPropBal
					from datafeeddb.dbo.dp1 dp1
					where dp1.propTypeCode = ''CH''
					and dp1.curDealBal &gt; 1
			</source>
		</type>
		<type name="Cnt">
			<aggregate>
				COUNT(loanID)
			</aggregate>
			<source>
				select dealName, ''CoopHousing'' as value_field, loanID
					from datafeeddb.dbo.dp1 dp1
					where dp1.propTypeCode = ''CH''
					and dp1.curDealBal &gt; 1
			</source>
		</type>
		<type name="Pct">
			<aggregate>
				sum(curPropBal)
			</aggregate>
			<post_field>
				*100/curDealBal
			</post_field>
			<source>
				select dealName, ''CoopHousing'' as value_field,  curDealBal, curPropBal
					from datafeeddb.dbo.dp1 dp1
					where dp1.propTypeCode = ''CH''
					and dp1.curDealBal &gt; 1
			</source>
		</type>
	</crosstab>

	<crosstab name="ASER Statistics">
		<field name="ASER" />
		<type name="Amt">
			<aggregate>
				SUM([sum])
			</aggregate>
			<source>
				select dealName, ''ASER'' as value_field,
					case
						when abs(appRedAmtTrustee) > 0 THEN abs(appRedAmtTrustee)
						else abs(mrAser) * 100 * pmtFreq / curCpn
					end as [sum]
					from  datafeeddb.dbo.dl1 dl1
					where (abs(mrAser) &gt; 0 or abs(appRedAmtTrustee) &gt; 0)
			</source>
		</type>
		<type name="Cnt">
			<aggregate>
				COUNT(loanID)
			</aggregate>
			<source>
				select dealName, ''ASER'' as value_field, loanID
					from  datafeeddb.dbo.dl1 dl1
					where (abs(mrAser) &gt; 0 or abs(appRedAmtTrustee) &gt; 0)
			</source>
		</type>
		<type name="Pct">
			<post_field>
				*100/curDealBal
			</post_field>
			<aggregate>
				MIN(ASERAmt)
			</aggregate>
			<source>
				select dealName as dealName, ''ASER'' as value_field, dfd.ASERAmt, curDealBal
					from datafeeddb.dbo.dl1 dl1
					inner join datafeeddb.dbo.dfDeal dfd on dl1.dealName = dfd.DealName
			</source>
		</type>
	</crosstab>

	<crosstab name="Maturity LTV">
		<field name="Maturity" />
		<type name="LTV">
			<post_field>
			</post_field>
			<aggregate>
				MIN(LTV)
			</aggregate>
			<source>
				select dealName, ''Maturity'' as value_field,
				dbo.noZeros(sum(isnull(secLoanBal,0)*isnull(balloonLTV,0)) / sum(isnull(secLoanBal,1))) as LTV
				from datafeeddb.dbo.dl1 dl1
				where balloonLTV &gt; 0
				group by dealName
			</source>
		</type>
	</crosstab>
	
	<crosstab name="Special Servicer">
		<field name="SpecialServicer" />
		<type name="Amt">
			<aggregate>
				SUM(curLoanBal)
			</aggregate>
			<source>
				select dealName, ''SpecialServicer'' as value_field, curLoanBal from datafeeddb.dbo.dl1 dl1
				where isnull(curMasterServReturnDt, ''19000101'') &lt; isnull(curSpecServTransDt, ''19000101'')
			</source>
		</type>
		<type name="Cnt">
			<aggregate>
				COUNT(loanID)
			</aggregate>
			<source>
				select dealName, ''SpecialServicer'' as value_field, loanID from datafeeddb.dbo.dl1 dl1
				where isnull(curMasterServReturnDt, ''19000101'') &lt; isnull(curSpecServTransDt, ''19000101'')
			</source>
		</type>
		<type name="Pct">
			<post_field>
				*100 / curDealBal
			</post_field>
			<aggregate>
				SUM(curLoanBal)
			</aggregate>
			<source>
				select dealName, ''SpecialServicer'' as value_field, curLoanBal, curDealBal from datafeeddb.dbo.dl1 dl1
				where isnull(curMasterServReturnDt, ''19000101'') &lt; isnull(curSpecServTransDt, ''19000101'')
			</source>
		</type>
	</crosstab>
		
	<crosstab name="Performing Special Servicer">
		<field name="PerformSpecialSrvcd" />
		<type name="Amt">
			<aggregate>
				SUM(curLoanBal)
			</aggregate>
			<source>
				select dealName, ''PerformSpecialSrvcd'' as value_field, curLoanBal from datafeeddb.dbo.dl1 dl1
				where isnull(curMasterServReturnDt, ''19000101'') &lt; isnull(curSpecServTransDt, ''19000101'')
				and curDlqCode in (''0'', ''A'', ''B'', ''4'')
			</source>
		</type>
		<type name="Cnt">
			<aggregate>
				COUNT(loanID)
			</aggregate>
			<source>
				select dealName, ''PerformSpecialSrvcd'' as value_field, loanID from datafeeddb.dbo.dl1 dl1
				where isnull(curMasterServReturnDt, ''19000101'') &lt; isnull(curSpecServTransDt, ''19000101'')
				and curDlqCode in (''0'', ''A'', ''B'', ''4'')
			</source>
		</type>
		<type name="Pct">
			<post_field>
				*100 / curDealBal
			</post_field>
			<aggregate>
				SUM(curLoanBal)
			</aggregate>
			<source>
				select dealName, ''PerformSpecialSrvcd'' as value_field, curLoanBal, curDealBal from datafeeddb.dbo.dl1 dl1
				where isnull(curMasterServReturnDt, ''19000101'') &lt; isnull(curSpecServTransDt, ''19000101'')
				and curDlqCode in (''0'', ''A'', ''B'', ''4'')
			</source>
		</type>
	</crosstab>

	<crosstab name="Delinquencies">
		<field name="Within30Day" />
		<field name="Delinq30Day" />
		<field name="Delinq60Day" />
		<field name="Delinq90Day" />
		<field name="CurrentExtendedBalloon" />
		<field name="Foreclosure" />
		<field name="REO" />
		<field name="DelinqUnknown" />
		<field name="NonPerfMatBalloon" />

		<type name="Amt">
			<aggregate>
				SUM(curLoanBal)
			</aggregate>
			<source>
				select dl.dealName, isnull(p.perfGroup, ''DelinqUnknown'') as value_field, dl.curLoanBal
				from datafeeddb.dbo.dl1 dl left outer join datafeeddb.dbo.perf_groups p on dl.curDlqCode = p.dlqCode
				where dl.curDealBal &gt; 1
			</source>
		</type>
		<type name="Cnt">
			<aggregate>
				COUNT(loanID)
			</aggregate>
			<source>
				select dl.dealName, isnull(p.perfGroup, ''DelinqUnknown'') as value_field, dl.loanID
				from datafeeddb.dbo.dl1 dl left outer join datafeeddb.dbo.perf_groups p on dl.curDlqCode = p.dlqCode
				where dl.curDealBal &gt; 1
			</source>
		</type>
		<type name="Pct">
			<post_field>
				*100 / curDealBal
			</post_field>
			<aggregate>
				SUM(curLoanBal)
			</aggregate>
			<source>
				select dl.dealName, isnull(p.perfGroup, ''DelinqUnknown'') as value_field, dl.curLoanBal, dl.curDealBal
				from datafeeddb.dbo.dl1 dl left outer join datafeeddb.dbo.perf_groups p on dl.curDlqCode = p.dlqCode
				where dl.curDealBal &gt; 1
			</source>
		</type>
	</crosstab>			

</root>'

------------------
--  parse the xml into a table 
---------------------
truncate table datafeeddb.dbo.xtabs

exec sp_xml_preparedocument @idoc OUTPUT,@xml

insert into datafeeddb.dbo.xtabs
select t.name, f.field, f.topx, t.type, t.aggregate, t.source, t.post_field
from openxml(@idoc, '/root/crosstab/type', 2)
with (
	name varchar(80) '../@name',
	type varchar(80) '@name',
	aggregate nvarchar(max) './aggregate',
	source nvarchar(max) './source',
	post_field varchar(160) './post_field'
	) t
cross join (
select * from openxml(@idoc, '/root/crosstab/field', 2)
 with (
	name varchar(80) '../@name',
	field varchar(80) '@name',
	topx varchar(5) '@topx'
 )
) f
where t.name = f.name

exec sp_xml_removedocument @idoc OUTPUT

--select * from xtabs


---------------------------------------------------------------------------------------------------------
  ---------------------------------  Pivots  --------------------------------------------------------
---------------------------------------------------------------------------------------------------------

--declare @type varchar(10), @field varchar(80), @name varchar(80), @lastName varchar(80), @values nvarchar(max), @datafeedFields nvarchar(max), @srcFields nvarchar(max)
--declare @aggregateClause nvarchar(max), @sourceClause nvarchar(max),  @sql nvarchar(max), @id int, @postField varchar(160), @topx int

----  step through our table performing the xtabs

set @id = 0;

declare fieldCursor CURSOR FORWARD_ONLY FOR
select type, name, isnull(topx, 0) as topx from datafeeddb.dbo.xtabs
where [aggregate] is not null and [source] is not null
group by name, type, topx;

open fieldCursor;

fetch next from fieldCursor into @type, @name, @topx;

WHILE @@FETCH_STATUS = 0
BEGIN
	select @values = NULL, @datafeedFields = NULL, @srcFields = NULL, @sourceClause = NULL, @aggregateClause = NULL, @postField = NULL

	select @name = name from datafeeddb.dbo.xtabs where name = @name and type = @type
	if (isnull(@lastName, '') <> @name)
		begin
			set @lastName = @name
			print '  ----------------------------------------------------------------------------------'
			print '------------   Performing crosstabs for '+@name+'   ------------------------------'
			print '  ----------------------------------------------------------------------------------'
		end

	select @id = @id + 1
	--print 'id ' + convert(varchar(20), @id) + ' type ' + @type + ' field ' +@name + ' aggregate ' + @aggregate + ' source ' + @source
	
	select @datafeedFields = 
		case
			when @type in ('Amt', 'Pct') then coalesce (@datafeedFields + ', ', '') + '[' + field + type + ']' + ' = round(p.['+ field + type + '], 2)'
			else coalesce (@datafeedFields + ', ', '') + '[' + field + type + ']' + ' = p.['+ field + type + ']'
		end
	from datafeeddb.dbo.xtabs where name = @name and type = @type and isnull(topx,0) = @topx
	
	select @values = coalesce (@values + ', ', '') + '[' + field + ']',
		@srcFields = coalesce (@srcFields + ', ', '') + '[' + field + ']' + isnull(post_field, '') + ' as ['+ field + type + ']' ,
		@aggregateClause = aggregate, @sourceClause=source
	from datafeeddb.dbo.xtabs where name = @name and type = @type  and isnull(topx,0) = @topx
	
	if (@topx > 0)
		begin
			select @sourceClause = replace(@sourceClause, '%x', @topx)
			
			select @sourceClause = replace(@sourceClause, '%field', ''''+field+'''')
			from datafeeddb.dbo.xtabs where name = @name and type = @type  and topx = @topx
		end
	
	--print @values;-- + convert(varchar(20), @id)
	--print @datafeedFields;-- + convert(varchar(20), @id)
	--print @srcFields;-- + convert(varchar(20), @id)
	print convert(varchar(20), @id)

set @sql =
'update datafeeddb.dbo.dfDeal set ' + @datafeedFields + ' from datafeeddb.dbo.dfDeal inner join (
select dealName, ' + @srcFields +
' FROM
(
'+@sourceClause+'
) as src
PIVOT
(
	' + @aggregateClause + '
	FOR value_field in ('+@values+')
) as pvt
)  as p on p.dealName = datafeeddb.dbo.dfDeal.DealName'

print @sql

exec (@sql)


	fetch next from fieldCursor into @type, @name, @topx
END;

close fieldCursor
deallocate fieldCursor

---------------------------------------------------------------------------------------------------------
  ---------------------------------  DEFAULT values  --------------------------------------------------------
---------------------------------------------------------------------------------------------------------

declare @el varchar(max)
declare @f varchar(max)
set @f = '[ASERAmt],[ASERCnt],[ASERPct],[CTLAmt],[CTLCnt],[CTLPct],[CoopHousingAmt],[CoopHousingCnt],[CoopHousingPct],[CurrentExtendedBalloonAmt],[CurrentExtendedBalloonCnt],[CurrentExtendedBalloonPct],[DefeaseAmt],[DefeaseCnt],[DefeasePct],[Delinq30DayAmt],[Delinq30DayCnt],[Delinq30DayPct],[Delinq60DayAmt],[Delinq60DayCnt],[Delinq60DayPct],[Delinq90DayAmt],[Delinq90DayCnt],[Delinq90DayPct],[DelinqUnknownAmt],[DelinqUnknownCnt],[DelinqUnknownPct],[FixedAmt],[FixedCnt],[FixedPct],[FloatingAmt],[FloatingCnt],[FloatingPct],[ForeclosureAmt],[ForeclosureCnt],[ForeclosurePct],[HealthcareAmt],[HealthcareCnt],[HealthcarePct],[HotelFullAmt],[HotelFullCnt],[HotelFullPct],[HotelLimitedAmt],[HotelLimitedCnt],[HotelLimitedPct],[HotelOtherAmt],[HotelOtherCnt],[HotelOtherPct],[IndustrialAmt],[IndustrialCnt],[IndustrialPct],[LockPct],[MaturityLTV],[MixedUseAmt],[MixedUseCnt],[MixedUsePct],[MobileHomeAmt],[MobileHomeCnt],[MobileHomePct],[MultifamilyAmt],[MultifamilyCnt],[MultifamilyPct],[NonPerfMatBalloonAmt],[NonPerfMatBalloonCnt],[NonPerfMatBalloonPct],[OfficeAmt],[OfficeCnt],[OfficePct],[OpenPct],[OtherAmt],[OtherCnt],[OtherPct],[PPPct],[PerformSpecialSrvcdAmt],[PerformSpecialSrvcdCnt],[PerformSpecialSrvcdPct],[REOAmt],[REOCnt],[REOPct],[RetailAnchoredAmt],[RetailAnchoredCnt],[RetailAnchoredPct],[RetailUnanchoredAmt],[RetailUnanchoredCnt],[RetailUnanchoredPct],[SelfStorageAmt],[SelfStorageCnt],[SelfStoragePct],[SpecialServicerAmt],[SpecialServicerCnt],[SpecialServicerPct],[State1Amt],[State1Pct],[State1PropCnt],[State2Amt],[State2Pct],[State2PropCnt],[State3Amt],[State3Pct],[State3PropCnt],[State4Amt],[State4Pct],[State4PropCnt],[State5Amt],[State5Pct],[State5PropCnt],[Top10LoansAmt],[Top15LoansAmt],[Top1DefeasedLoanAmt],[Top1DefeasedLoanPct],[Top1LoansAmt],[Top2DefeasedLoanAmt],[Top2DefeasedLoanPct],[Top3DefeasedLoanAmt],[Top3DefeasedLoanPct],[Top5LoansAmt],[TypeUndefinedAmt],[TypeUndefinedCnt],[TypeUndefinedPct],[WarehouseAmt],[WarehouseCnt],[WarehousePct],[WatchlistAmt],[WatchlistCnt],[WatchlistPct],[Within30DayAmt],[Within30DayCnt],[Within30DayPct],[YMPct],'
while (len(@f) > 1)
begin
	
	select @el = substring(@f, 1, patindex('%,%', @f) - 1)
	select @f = replace(@f, @el + ',', '')
	
	set @sql = 'UPDATE datafeeddb.dbo.dfDeal set ' + @el + ' = 0 WHERE ' + @el + ' IS NULL'
	
	--print @sql
	
	exec (@sql)
end


---------------------------------------
			end
			
		COMMIT TRAN
		
	END TRY
	BEGIN CATCH
		SET @errorText = 'ERROR:' + + ERROR_MESSAGE() + char(13) + ' ON SQL:' + @sql + char(13) + @e2
		-- Test whether the transaction is committable.
		IF @@TRANCOUNT = 1
			ROLLBACK TRAN
		IF @@TRANCOUNT > 1
			COMMIT TRAN

		RAISERROR(@errorText,11,0,@SERVERID,@DBNAME,@SPID)
	END CATCH
	--RETURN @errorText
END


GO

--
--   example generated sql
--

 ----------------------------------------------------------------------------------
------------   Performing crosstabs for ASER Statistics   ------------------------------
  ----------------------------------------------------------------------------------
1
Executing sql:update datafeeddb.dbo.dfDealLoad set [ASERAmt] = round(p.[ASERAmt], 2) from datafeeddb.dbo.dfDealLoad inner join (
select dealName, [ASER] as [ASERAmt] FROM
(
select dealName, 'ASER' as value_field,
          case
            when abs(appRedAmtTrustee) > 0 THEN abs(appRedAmtTrustee)
            else abs(mrAser) * 100 * pmtFreq / curCpn
          end as [sum]
          from  datafeeddb.dbo.work_dealLoan dl1
          where (abs(mrAser) > 0 or abs(appRedAmtTrustee) > 0)
          and dl1.curDealBal > 0.15
          and dl1.curLoanBal > 0.015
) as src
PIVOT
(
  SUM([sum])
  FOR value_field in ([ASER])
) as pvt
)  as p on p.dealName = datafeeddb.dbo.dfDealLoad.DealName
2
Executing sql:update datafeeddb.dbo.dfDealLoad set [ASERCnt] = p.[ASERCnt] from datafeeddb.dbo.dfDealLoad inner join (
select dealName, [ASER] as [ASERCnt] FROM
(
select dealName, 'ASER' as value_field, loanID
          from  datafeeddb.dbo.work_dealLoan dl1
          where (abs(mrAser) > 0 or abs(appRedAmtTrustee) > 0)
          and dl1.curDealBal > 0.15
          and dl1.curLoanBal > 0.015
) as src
PIVOT
(
  COUNT(loanID)
  FOR value_field in ([ASER])
) as pvt
)  as p on p.dealName = datafeeddb.dbo.dfDealLoad.DealName
3
Executing sql:update datafeeddb.dbo.dfDealLoad set [ASERPct] = round(p.[ASERPct], 2) from datafeeddb.dbo.dfDealLoad inner join (
select dealName, [ASER]*100/curDealBal as [ASERPct] FROM
(
select dealName as dealName, 'ASER' as value_field, dfd.ASERAmt, curDealBal
          from datafeeddb.dbo.work_dealLoan dl1
          inner join datafeeddb.dbo.dfDealLoad dfd on dl1.dealName = dfd.DealName
          where curDealBal > 1.5
          and dl1.curLoanBal > 0.015
) as src
PIVOT
(
  MIN(ASERAmt)
  FOR value_field in ([ASER])
) as pvt
)  as p on p.dealName = datafeeddb.dbo.dfDealLoad.DealName
