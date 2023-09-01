SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;
/*
--如果每日派發多次，僅取當日最後一次 (sequenceId唯一)
select distinct sequenceId from TP_PeggyTsai..MDAssignBDSalesTrace
select * from TP_PeggyTsai..MDAssignBDSalesTrace

--如果成交多筆，則取STV>0的第一筆
*/
declare @mgm int =1;
DECLARE @tdate datetime,
					@ssdate datetime,
					@sdate datetime, 
					@edate datetime,
					@ydate datetime;

------------------------------------------------------
/*--TDate設定為撈取日期-1天，如要修改撈取日，則改此設定'即可---*/
SET @tdate =convert(date,getdate()-1);

------------------------------------------------------

SET @ssdate= dateadd(m, datediff(M,0,@tdate)-3,0) ;	--3個月前第一天	    
SET @sdate= dateadd(m, datediff(m,0,@tdate),0);									--MTD(至昨日)當月第一天
SET @edate= DATEADD(DAY,1,@tdate);												--當天
SET @ydate= @tdate;																--昨天
DECLARE @emonth INT = month(@edate)



----------Assign------------
IF OBJECT_ID('tempdb.dbo.#Assign','U')IS NOT NULL
DROP TABLE #Assign;

select 
  MDA. BrandID as BrandID	--首次媒體
, MDA. LeadBrandID			--首次媒體
, MDA. lead_sn
, MDA. SocialLeadId
, MDA. SocialType as SocialType
, MDA. inpdate as inpdate	--首次媒體
, MDA. market_media_sn	 as market_media_sn	--首次媒體
, MDA. AssignDate as AssignDate_first
, MDA. assign_level as Assign_Level_first
, MDA. agent as Agent_first  
, case when  MDA.SocialLeadId is null then Lead.URL else sl.URL_scrm end as URL
, SALES_record.SalesLevelID  as SalesLevel_first
, SALES_record.SalesBrand as salesbrand_first
, SALES_record.SalesRegionManagerEname as SalesRegionManagerEname_first
, SALES_record.team as Deptshort_first
, SALES_record.SalesRegionDescrCN as SalesRegionDescrCN_first
, SALES_record.ManagerEname as ManagerEname_first
, SALES_record.salesname as Salesname_first
, Media.L1_CName
, Media.L2_CName
, Media.L3_CName

into #Assign

from (
		select 
			  MDA. BrandID as BrandID	--首次媒體
			, MDA. LeadBrandID			--首次媒體
			, MDA. lead_sn
			, MDA. SocialLeadId
			, MDA. SocialType as SocialType
			, MDA. inpdate as inpdate	--首次媒體
			, MDA. market_media_sn	 as market_media_sn	--首次媒體
			, MDA. AssignDate 
			, MDA. assign_level 
			, MDA. agent 
			, ROW_NUMBER()over(partition by MDA.inpdate,MDA.lead_sn,MDA.socialleadid,MDA.market_media_sn order by MDA.assigndate) as rn_assign
		from bi_edw.dbo.FactMDAssignMediaTrace	as MDA with(nolock)
		where 1=1
--留名單 & 派發時間皆在預設時間範圍區間內
		and MDA.inpdate >=@ssdate
		and MDA.inpdate <@edate					--MTD	
		and MDA.assigndate >=@ssdate
		and MDA.assigndate <@edate				--MTD	
		and MDA.BrandID in (1,3)
		or
		(MDA.inpdate >='2020-09-01'
		and MDA.inpdate <'2020-10-01'			--MTD	
		and MDA.assigndate >='2020-09-01'
		and MDA.assigndate <'2020-10-01'				--MTD	
		and MDA.BrandID in (1,3))
) as MDA

LEFT JOIN bi_edw.dbo.DIMLEAD as Lead with(nolock)
	on MDA.lead_sn = Lead.lead_sn 

LEFT JOIN (
select YMD,SalesLevelID,SalesBrand, Team,sales_sn,ps.salesfname+' '+ps.saleslname as salesname ,ps2.*
	from bi_edw.dbo.FactSalesRecordFromPS as record WITH(NOLOCK)  
	left join bi_edw.dbo.FactSalesInfoFromPS as ps WITH(NOLOCK) on record.sales_sn=ps.IMSSalesSn
	left join (	select distinct DeptId,SalesRegionManagerEname,SalesRegionDescrCN,ManagerEname from bi_edw.dbo.FactSalesInfoFromPS WITH(NOLOCK) where Location in (N'臺北',N'台中') and LeaveDate is null and DeptTW like N'%業務部%') as ps2 on record. PSDeptId = ps2.DeptId
	where	YMD >=@ssdate and YMD <@edate
	) as SALES_record
		on MDA. agent = SALES_record.sales_sn
		and convert(date,MDA.AssignDate)=CONVERT(date,SALES_record.YMD)

LEFT JOIN bi_edw.dbo.DimMediaforTrace as Media with(nolock)
	on MDA.market_media_sn=Media.mediakey


LEFT JOIN bi_edw.dbo.DimSocialLead AS sl WITH (NOLOCK) 
	on MDA.socialleadid = sl.SocialLeadId_binary

where MDA.rn_assign=1


IF OBJECT_ID('tempdb.dbo.#demo','U')IS NOT NULL
DROP TABLE #demo;

select *
into #demo
FROM   (
       SELECT 
              A.[lead_sn],
              A.[market_media_sn],
              A.[media_brand_id],
              A.[Inpdate],
              A.ReservTime,
              A.shouldShowTime,
              case when B.[status]  = 3 THEN ISNULL(B.[showTime], A.[showTime]) else NULL end AS showTime,
              A.[db_source]
       FROM   (
              SELECT 
                     [lead_sn],
                     MediaKey_FirstMedia as[market_media_sn],
                     inpdate_FirstMedia as[Inpdate],
                     BrandID_FirstMedia as[media_brand_id],
                     MAX([ReservTime]) AS ReservTime,
                     MAX([shouldShowTime]) AS shouldShowTime,
                     MAX([showTime]) AS showTime,
                     [db_source]
              FROM   bi_edw.dbo.[FactMDDemoMediaTrace] WITH (NOLOCK)
              GROUP BY                                   
                     --close_rn,
                     [lead_sn],
                     MediaKey_FirstMedia ,
                     inpdate_FirstMedia ,
                     BrandID_FirstMedia ,
                     [db_source]
              ) A
              LEFT JOIN 
              ( 
              SELECT  lead_sn,showTime, /*出席日期*/ [status],	/*出席狀況*/ STATUS_byCon, db_source,MediaKey_FirstMedia as market_media_sn,
                     ROW_NUMBER() OVER(PARTITION BY close_rn,lead_sn,db_source ORDER BY [ReservTime]/*預約日期*/ DESC ) AS rn
              FROM   bi_edw.dbo.[FactMDDemoMediaTrace] WITH (NOLOCK)
              WHERE  ([status] = 3 OR [STATUS_byCon] = 3 )
                      AND show_rn = 1
			   ) B
              ON   B.lead_sn = A.lead_sn
              AND b.db_source = a.db_source
              AND b.[market_media_sn] = a.[market_media_sn]
 ) AS demo

WHERE  demo.media_brand_id in (1,3)
       AND
       demo.Inpdate >= @ssdate
       AND
       demo.Inpdate < @edate
	   or
	   ( demo.media_brand_id in (1,3)
       AND
       demo.Inpdate >= '2020-09-01'
       AND
       demo.Inpdate < '2020-10-01')


----------Signup & STV----------

IF OBJECT_ID('TempDb.dbo.#STV','U') IS NOT NULL
BEGIN
    DROP TABLE #STV;
END;
    SELECT 
	 stv.Product_BrandID
	, stv.lead_sn
	, stv.contract_sn
	, stv.rec_time as FDSellingDate
	, stv.market_media_sn_ByFistMedia as market_media_sn
	, stv.inpdateBy_FistMedia as inpdate
    ,SUM( CASE WHEN rec_type = N'SuccessCNT' THEN rec_count ELSE 0 END) AS Signup
    ,SUM( CASE WHEN rec_type = N'STV' THEN rec_amount ELSE 0 END ) AS stv
	,ROW_NUMBER()OVER(partition by lead_sn,inpdateBy_FistMedia,stv.market_media_sn_ByFistMedia order by rec_time desc) as RN_stv
    INTO #STV
    FROM bi_edw.dbo.FactMDSTVMediaTrace AS stv  with(nolock)
    WHERE STV.BrandId in (1,3)
    AND
	 ( 
		(stv.inpdateBy_FistMedia >= @ssdate AND stv.inpdateBy_FistMedia < @edate) AND (stv.rec_time >= @ssdate AND stv.rec_time < @edate) 
	 or
		(stv.inpdateBy_FistMedia >= '2020-09-01' AND stv.inpdateBy_FistMedia < '2020-10-01') AND (stv.rec_time >= '2020-09-01' AND stv.rec_time < '2020-10-01')
	 ) ---A date
    AND STVTypeN <> N'續購' 
    AND STVTypeN <> N'展延' 
    AND STVTypeN <> N'轉讓' 
    AND STVTypeN not like N'%延期轉讓%'
	AND rec_type in ('SuccessCNT','STV')
    GROUP BY  
	stv.Product_BrandID
--	, stv.BrandID
	, stv.lead_sn
	, stv.contract_sn
	, stv.rec_time
	, stv.market_media_sn_ByFistMedia
	, stv.inpdateBy_FistMedia





/*----------------------------------------------------------------------------
		SUMMARY
----------------------------------------------------------------------------*/
IF OBJECT_ID('TempDb.dbo.#tmp','U') IS NOT NULL
DROP TABLE #tmp;

select #Assign.*
, #STV. FDSellingDate 
, #STV. STV
, #STV. SignUp
, #STV. Contract_sn
into #tmp
from
(
	select *
	from 
	(
	select #Assign.LeadBrandId
	,#Assign.BrandId
	,#Assign.lead_sn
	,#Assign.SocialLeadId
	,#Assign.SocialType
	,#Assign.URL
	--,#Assign.DBSource
	,#Assign.Inpdate
	,#Assign.market_media_sn
	,#Assign.L1_Cname
	,#Assign.L2_Cname
	,#Assign.L3_Cname
--	,case when #Assign.inpdate_first is null then 'N' else 'Y' end as 'ISNEW'	--inpdate_first & inpdate 會相等
	,#Assign.assignDate_first
	,#Assign.assign_level_first
--	,#Assign.SequenceId_first
	,#Assign.Agent_first
	,#Assign.SalesBrand_first
	,#Assign.deptshort_first
	,#Assign.SalesLevel_first
	,#Assign.managerEname_first
	,#Assign.SalesRegionDescrCN_first
	,#Assign.SalesRegionManagerEname_first
	,#Assign.Salesname_first
--	,#Assign.Isatpost_first
	,#Demo.ReservTime
	,#Demo.ShouldShowTime
	,#Demo.ShowTime
	,row_number()over(partition by #Assign.lead_sn,#Assign.socialleadid
								  ,#Assign.market_media_sn
								  ,#Assign.Inpdate
					  order by case when #Demo.ShowTime is not null then 1
					  else 0 end desc,
					  case when #Demo.ReservTime is not null then #Demo.ReservTime else #Assign.assignDate_first end desc
					  )R	
					  --如未預約紀錄，則抓最新派發時間;如有預約紀錄，則抓最新預約/應出席時間;如有出席紀錄，則抓有出席的預約/出席/派發時間
	from #Assign
	left join #Demo
					ON #Assign.Lead_sn=#Demo.Lead_sn
					AND #Assign.Market_Media_SN=#Demo.Market_Media_SN
					AND #Assign.Inpdate=#Demo.Inpdate			

	)t 
	where R=1  
	
) #Assign
left join #STV as #STV
				ON #Assign.Lead_sn=#STV.Lead_sn
				AND #Assign.Market_Media_SN=#STV.market_media_sn
				AND #Assign.Inpdate=#STV.Inpdate
				AND #STV.RN_stv=1



/*-----------------------------------------*/

IF OBJECT_ID('TempDb.dbo.#MDassigntoBDtrace','U') IS NOT NULL
BEGIN
    DROP TABLE #MDassigntoBDtrace;
END;

SELECT case when BrandId = 1 then 'TutorABC(TP)' else 'tutorJr' end as 媒體品牌
	, case when L1_CName like N'網路%' then N'網路廣告(' + L2_CName +')'
		   when L1_CName = N'其他媒體' then L1_CName +'('+ L2_CName +')'
		   else L1_CName
		   end as 媒體渠道
	,case when L1_CName in (N'客戶介紹',N'轉介紹') then 1 else 0 end as ismgm
	,datediff(day,assignDate_first,FDSellingDate)as Signupterm
	,case when year(ReservTime)		= year(inpdate) and  month(ReservTime)		= month(inpdate) then ReservTime end ReservTime_A
	,case when year(shouldshowtime) = year(inpdate) and  month(shouldshowtime)  = month(inpdate) then shouldshowtime end shouldshowtime_A
	,case when year(showTime)			= year(inpdate) and  month(showTime)			= month(inpdate) then showTime end showTime_A
	,case when year(FDSellingdate)		= year(inpdate) and  month(FDSellingdate)		= month(inpdate) then FDSellingdate end FDSellingdate_A
	,case when year(FDSellingdate)		= year(inpdate) and  month(FDSellingdate)		= month(inpdate) then SignUp end SignUp_A
	,case when year(FDSellingdate)		= year(inpdate) and  month(FDSellingdate)		= month(inpdate) then STV end STV_A
	,YEAR(inpdate) AS inp_year
	,MONTH(inpdate) AS inp_month
	,*	
into #MDassigntoBDtrace
from #tmp (nolock)


/*-----------------------------------------*/

IF OBJECT_ID('TempDb.dbo.#MDassigntoBDtrace_SUM','U') IS NOT NULL
BEGIN
    DROP TABLE #MDassigntoBDtrace_SUM;
END;



	select 媒體品牌, 媒體渠道
			, count (Agent_first) as 派發數
			, sum (case when ReservTime_A is not null then 1 else 0 end) as 預約數
			, sum (case when ShouldShowTime_A is not null then 1 else 0 end) as 應出席數
			, sum (case when ShowTime_A is not null then 1 else 0 end) as 出席數
			, ISNULL(sum (SignUp_A),0) as 成交數
			, ISNULL(sum (STV_A),0) as STV
			, case when  sum (SignUp_A) =0 then 0 else ISNULL(sum (STV_A)/sum (SignUp_A),0) end as ATS
			, ISNULL(sum (SignUp_A)*1.0 / sum (case when assignDate_first is not null then 1 else 0 end), 0)as 轉換率
			, case when  sum (SignUp_A) =0 then 0 else sum (Signupterm)*1.0 / sum (SignUp_A) end as 成交週期
			, case when count (Agent_first)=0 then 0 else sum(STV_A)*1.0/count (Agent_first) end as STV_perassign
	into #MDassigntoBDtrace_SUM
	from #MDassigntoBDtrace  
	--where leadbrandid in (1,3) and inpdate <@sdate
	where brandid in (1,3) and inpdate <@sdate
	group by 媒體品牌, 媒體渠道



;

IF OBJECT_ID('TempDb.dbo.#final1','U') IS NOT NULL
BEGIN
    DROP TABLE #final1;
END;


select 

 deptshort_first 
 ,ismgm
, count( 1 )as 派發數
, sum( case when ReservTime is null then 0 else 1 end)as 預約數
, sum( case when ShouldShowTime is null then 0 else 1 end)as 應出席數
, sum( case when ShowTime is null then 0 else 1 end)as 出席數
, ISNULL (sum (a.STV),0) as STV
, ISNULL (sum (a.SignUp),0) as 成交數
, case when count( lead_sn ) = 0 then null else ISNULL (sum (a.SignUp),0)*1.0 / ISNULL (count( lead_sn ),0) end  as '轉換率'
, sum(STV_perassign) as 預估STV

into #final1

from #MDassigntoBDtrace a (nolock)

left join #MDassigntoBDtrace_SUM b
on a. 媒體品牌 = b.媒體品牌  and  a. 媒體渠道 = b.媒體渠道

where a. Inpdate >= @sdate and a. Inpdate < @edate
--and a. assignDate_first >= @sdate and a. assignDate_first < @edate
and year(Inpdate) = year(@ydate) and month(Inpdate) = month(@ydate) --當月名單
and deptshort_first in (select  distinct DeptTW from bi_edw.dbo.FactSalesInfoFromPS with(nolock) where LeaveDate is null and DeptTW like N'TBD_業務部_BD%' )
--and a. 媒體品牌+a. 媒體渠道 in 
--(
--select 媒體品牌+媒體渠道
--from #MDassigntoBDtrace_SUM
--where (媒體渠道 ='Official Website'
--	or 媒體渠道 like N'網路%'
--	or 媒體渠道 like N'%tutorJr部落格%'
--	or 媒體渠道 in (N'講座和實體活動',N'地推活動') --offline
--	or 媒體渠道 in (N'社群媒體') 
--	--OR 媒體渠道 IN (N'客戶介紹', N'轉介紹') 
--	)
--)
group by 
 deptshort_first
 ,ismgm

IF OBJECT_ID('TempDb.dbo.#bind','U') IS NOT NULL
BEGIN
    DROP TABLE #bind;
END;

select 
		Deptshort_first
		,ismgm
		,count(isnull(BindLeadTime_new,bindleadtime)) as bind_cnt
		--,count(case when isnull(BindLeadTime_new,bindleadtime)<AssignDate_first then 1 else null end) as bind_cnt_before
		,count(case when isnull(BindLeadTime_new,bindleadtime)>dateadd(minute,1,AssignDate_first) and isnull(BindLeadTime_new,bindleadtime)<=dateadd(dd,30,AssignDate_first) then 1 else null end) as bind_cnt_after30
		--,count(case when isnull(BindLeadTime_new,bindleadtime)>AssignDate and isnull(BindLeadTime_new,bindleadtime)<=dateadd(dd,30,AssignDate) then 1 else null end) as bind_cnt_after30
into #bind
from (
	select 
		lead_sn
		, Deptshort_first
		, ismgm,AssignDate_first 
	from 
		#MDassigntoBDtrace 
	where 
		Inpdate >= @sdate 
		and Inpdate < @edate 
		and Inp_year = 2021 
		and Inp_month = @emonth
) a
left join (select lead_sn,bindleadtime,BindLeadTime_new from  bi_edw.dbo.DimSocialLead  with(nolock)  where lead_sn in (select distinct lead_sn from #MDassigntoBDtrace)) as dsl
	on a.lead_sn=dsl.lead_sn
group by 
		Deptshort_first
		,ismgm

		IF OBJECT_ID('TempDb.dbo.#final_this','U') IS NOT NULL
BEGIN
    DROP TABLE #final_this;
END;
		
select #final1.Deptshort_first
, #final1.ismgm
,派發數
,預約數
,應出席數
,出席數
,STV
,成交數
,#bind.bind_cnt 
,預估STV
	,case when 派發數=0 then 0 else 預約數*1.0/派發數 end as 預約率
	,case when 應出席數=0 then 0 else 出席數*1.0/應出席數 end as 出席率
	,case when 出席數=0 then 0 else 成交數*1.0/出席數 end as 成交率
	,case when 派發數=0 then 0 else 預約數*1.0/派發數 end as 綁定率
	,case when 預估STV=0 then 0 else STV*1.0/預估STV end as 達成率
into #final_this
from #final1
left join #bind on #final1.Deptshort_first=#bind.Deptshort_first and #final1.ismgm = #bind. ismgm 
where #final1.ismgm=@mgm
order by 2,1	;

		
------------------------------------------------------
/*--TDate設定為撈取日期-1天，如要修改撈取日，則改此設定'即可---*/
SET @tdate =dateadd(dd,-1,dateadd(m, datediff(m,0,getdate()),0))--convert(date,getdate()-1);
------------------------------------------------------

SET @ssdate= dateadd(m, datediff(M,0,@tdate)-3,0) ;	--3個月前第一天	    
SET @sdate= dateadd(m, datediff(m,0,@tdate),0);									--MTD(至昨日)當月第一天
SET @edate= DATEADD(DAY,1,@tdate);												--當天
SET @ydate= @tdate;																--昨天




----------Assign------------
IF OBJECT_ID('tempdb.dbo.#Assign_','U')IS NOT NULL
DROP TABLE #Assign_;

select 
  MDA. BrandID as BrandID	--首次媒體
, MDA. LeadBrandID			--首次媒體
, MDA. lead_sn
, MDA. SocialLeadId
, MDA. SocialType as SocialType
, MDA. inpdate as inpdate	--首次媒體
, MDA. market_media_sn	 as market_media_sn	--首次媒體
, MDA. AssignDate as AssignDate_first
, MDA. assign_level as Assign_Level_first
, MDA. agent as Agent_first  
, case when  MDA.SocialLeadId is null then Lead.URL else sl.URL_scrm end as URL
, SALES_record.SalesLevelID  as SalesLevel_first
, SALES_record.SalesBrand as salesbrand_first
, SALES_record.SalesRegionManagerEname as SalesRegionManagerEname_first
, SALES_record.team as Deptshort_first
, SALES_record.SalesRegionDescrCN as SalesRegionDescrCN_first
, SALES_record.ManagerEname as ManagerEname_first
, SALES_record.salesname as Salesname_first
, Media.L1_CName
, Media.L2_CName
, Media.L3_CName

into #Assign_

from (
		select 
			  MDA. BrandID as BrandID	--首次媒體
			, MDA. LeadBrandID			--首次媒體
			, MDA. lead_sn
			, MDA. SocialLeadId
			, MDA. SocialType as SocialType
			, MDA. inpdate as inpdate	--首次媒體
			, MDA. market_media_sn	 as market_media_sn	--首次媒體
			, MDA. AssignDate 
			, MDA. assign_level 
			, MDA. agent 
			, ROW_NUMBER()over(partition by MDA.inpdate,MDA.lead_sn,MDA.socialleadid,MDA.market_media_sn order by MDA.assigndate) as rn_assign
		from bi_edw.dbo.FactMDAssignMediaTrace	as MDA with(nolock)
		where 1=1
--留名單 & 派發時間皆在預設時間範圍區間內
		and MDA.inpdate >=@ssdate
		and MDA.inpdate <@edate					--MTD	
		and MDA.assigndate >=@ssdate
		and MDA.assigndate <@edate				--MTD	
		and MDA.BrandID in (1,3)
		or
		(MDA.inpdate >='2020-09-01'
		and MDA.inpdate <'2020-10-01'			--MTD	
		and MDA.assigndate >='2020-09-01'
		and MDA.assigndate <'2020-10-01'				--MTD	
		and MDA.BrandID in (1,3))
) as MDA

LEFT JOIN bi_edw.dbo.DIMLEAD as Lead with(nolock)
	on MDA.lead_sn = Lead.lead_sn 

LEFT JOIN (
select YMD,SalesLevelID,SalesBrand, Team,sales_sn,ps.salesfname+' '+ps.saleslname as salesname ,ps2.*
	from bi_edw.dbo.FactSalesRecordFromPS as record WITH(NOLOCK)  
	left join bi_edw.dbo.FactSalesInfoFromPS as ps WITH(NOLOCK) on record.sales_sn=ps.IMSSalesSn
	left join (	select distinct DeptId,SalesRegionManagerEname,SalesRegionDescrCN,ManagerEname from bi_edw.dbo.FactSalesInfoFromPS WITH(NOLOCK) where Location in (N'臺北',N'台中') and LeaveDate is null and DeptTW like N'%業務部%') as ps2 on record. PSDeptId = ps2.DeptId
	where	YMD >=@ssdate and YMD <@edate
	) as SALES_record
		on MDA. agent = SALES_record.sales_sn
		and convert(date,MDA.AssignDate)=CONVERT(date,SALES_record.YMD)

LEFT JOIN bi_edw.dbo.DimMediaforTrace as Media with(nolock)
	on MDA.market_media_sn=Media.mediakey


LEFT JOIN bi_edw.dbo.DimSocialLead AS sl WITH (NOLOCK) 
	on MDA.socialleadid = sl.SocialLeadId_binary

where MDA.rn_assign=1


IF OBJECT_ID('tempdb.dbo.#demo_','U')IS NOT NULL
DROP TABLE #demo_;

select *
into #demo_
FROM   (
       SELECT 
              A.[lead_sn],
              A.[market_media_sn],
              A.[media_brand_id],
              A.[Inpdate],
              A.ReservTime,
              A.shouldShowTime,
              case when B.[status]  = 3 THEN ISNULL(B.[showTime], A.[showTime]) else NULL end AS showTime,
              A.[db_source]
       FROM   (
              SELECT 
                     [lead_sn],
                     MediaKey_FirstMedia as[market_media_sn],
                     inpdate_FirstMedia as[Inpdate],
                     BrandID_FirstMedia as[media_brand_id],
                     MAX([ReservTime]) AS ReservTime,
                     MAX([shouldShowTime]) AS shouldShowTime,
                     MAX([showTime]) AS showTime,
                     [db_source]
              FROM   bi_edw.dbo.[FactMDDemoMediaTrace] WITH (NOLOCK)
              GROUP BY                                   
                     --close_rn,
                     [lead_sn],
                     MediaKey_FirstMedia ,
                     inpdate_FirstMedia ,
                     BrandID_FirstMedia ,
                     [db_source]
              ) A
              LEFT JOIN 
              ( 
              SELECT  lead_sn,showTime, /*出席日期*/ [status],	/*出席狀況*/ STATUS_byCon, db_source,MediaKey_FirstMedia as market_media_sn,
                     ROW_NUMBER() OVER(PARTITION BY close_rn,lead_sn,db_source ORDER BY [ReservTime]/*預約日期*/ DESC ) AS rn
              FROM   bi_edw.dbo.[FactMDDemoMediaTrace] WITH (NOLOCK)
              WHERE  ([status] = 3 OR [STATUS_byCon] = 3 )
                      AND show_rn = 1
			   ) B
              ON   B.lead_sn = A.lead_sn
              AND b.db_source = a.db_source
              AND b.[market_media_sn] = a.[market_media_sn]
 ) AS demo

WHERE  demo.media_brand_id in (1,3)
       AND
       (
		demo.Inpdate >= @ssdate AND demo.Inpdate < @edate
	   or
		demo.Inpdate >= '2020-09-01' AND demo.Inpdate < '2020-10-01'
	   )
	   and (demo.shouldShowTime>=@ssdate and shouldShowTime<@edate ) 

----------Signup & STV----------

IF OBJECT_ID('TempDb.dbo.#STV_','U') IS NOT NULL
BEGIN
    DROP TABLE #STV_;
END;
    SELECT 
	 stv.Product_BrandID
	, stv.lead_sn
	, stv.contract_sn
	, stv.rec_time as FDSellingDate
	, stv.market_media_sn_ByFistMedia as market_media_sn
	, stv.inpdateBy_FistMedia as inpdate
    ,SUM( CASE WHEN rec_type = N'SuccessCNT' THEN rec_count ELSE 0 END) AS Signup
    ,SUM( CASE WHEN rec_type = N'STV' THEN rec_amount ELSE 0 END ) AS stv
	,ROW_NUMBER()OVER(partition by lead_sn,inpdateBy_FistMedia,stv.market_media_sn_ByFistMedia order by rec_time desc) as RN_stv
    INTO #STV_
    FROM bi_edw.dbo.FactMDSTVMediaTrace AS stv  with(nolock)
    WHERE STV.BrandId in (1,3)
    AND
	 ( 
		(stv.inpdateBy_FistMedia >= @ssdate AND stv.inpdateBy_FistMedia < @edate) AND (stv.rec_time >= @ssdate AND stv.rec_time < @edate) 
	 or
		(stv.inpdateBy_FistMedia >= '2020-09-01' AND stv.inpdateBy_FistMedia < '2020-10-01') AND (stv.rec_time >= '2020-09-01' AND stv.rec_time < '2020-10-01')
	 ) ---A date
    AND STVTypeN <> N'續購' 
    AND STVTypeN <> N'展延' 
    AND STVTypeN <> N'轉讓' 
    AND STVTypeN not like N'%延期轉讓%'
	AND rec_type in ('SuccessCNT','STV')
    GROUP BY  
	stv.Product_BrandID
--	, stv.BrandID
	, stv.lead_sn
	, stv.contract_sn
	, stv.rec_time
	, stv.market_media_sn_ByFistMedia
	, stv.inpdateBy_FistMedia





/*----------------------------------------------------------------------------
		SUMMARY
----------------------------------------------------------------------------*/
IF OBJECT_ID('TempDb.dbo.#tmp_','U') IS NOT NULL
DROP TABLE #tmp_;

select #Assign.*
, #STV. FDSellingDate 
, #STV. STV
, #STV. SignUp
, #STV. Contract_sn
into #tmp_
from
(
	select *
	from 
	(
	select #Assign.LeadBrandId
	,#Assign.BrandId
	,#Assign.lead_sn
	,#Assign.SocialLeadId
	,#Assign.SocialType
	,#Assign.URL
	--,#Assign.DBSource
	,#Assign.Inpdate
	,#Assign.market_media_sn
	,#Assign.L1_Cname
	,#Assign.L2_Cname
	,#Assign.L3_Cname
--	,case when #Assign.inpdate_first is null then 'N' else 'Y' end as 'ISNEW'	--inpdate_first & inpdate 會相等
	,#Assign.assignDate_first
	,#Assign.assign_level_first
--	,#Assign.SequenceId_first
	,#Assign.Agent_first
	,#Assign.SalesBrand_first
	,#Assign.deptshort_first
	,#Assign.SalesLevel_first
	,#Assign.managerEname_first
	,#Assign.SalesRegionDescrCN_first
	,#Assign.SalesRegionManagerEname_first
	,#Assign.Salesname_first
--	,#Assign.Isatpost_first
	,#Demo.ReservTime
	,#Demo.ShouldShowTime
	,#Demo.ShowTime
	,row_number()over(partition by #Assign.lead_sn,#Assign.socialleadid
								  ,#Assign.market_media_sn
								  ,#Assign.Inpdate
					  order by case when #Demo.ShowTime is not null then 1
					  else 0 end desc,
					  case when #Demo.ReservTime is not null then #Demo.ReservTime else #Assign.assignDate_first end desc
					  )R	
					  --如未預約紀錄，則抓最新派發時間;如有預約紀錄，則抓最新預約/應出席時間;如有出席紀錄，則抓有出席的預約/出席/派發時間
	from #Assign_ as #assign
	left join #Demo_ as #demo
					ON #Assign.Lead_sn=#Demo.Lead_sn
					AND #Assign.Market_Media_SN=#Demo.Market_Media_SN
					AND #Assign.Inpdate=#Demo.Inpdate			

	)t 
	where R=1  
	
) #Assign
left join #STV_ as #STV
				ON #Assign.Lead_sn=#STV.Lead_sn
				AND #Assign.Market_Media_SN=#STV.market_media_sn
				AND #Assign.Inpdate=#STV.Inpdate
				AND #STV.RN_stv=1



/*-----------------------------------------*/

IF OBJECT_ID('TempDb.dbo.#MDassigntoBDtrace_','U') IS NOT NULL
BEGIN
    DROP TABLE #MDassigntoBDtrace_;
END;

SELECT case when BrandId = 1 then 'TutorABC(TP)' else 'tutorJr' end as 媒體品牌
	, case when L1_CName like N'網路%' then N'網路廣告(' + L2_CName +')'
		   when L1_CName = N'其他媒體' then L1_CName +'('+ L2_CName +')'
		   else L1_CName
		   end as 媒體渠道
	,case when L1_CName in (N'客戶介紹',N'轉介紹') then 1 else 0 end as ismgm
	,datediff(day,assignDate_first,FDSellingDate)as Signupterm
	,case when year(ReservTime)		= year(inpdate) and  month(ReservTime)		= month(inpdate) then ReservTime end ReservTime_A
	,case when year(shouldshowtime) = year(inpdate) and  month(shouldshowtime)  = month(inpdate) then shouldshowtime end shouldshowtime_A
	,case when year(showTime)			= year(inpdate) and  month(showTime)			= month(inpdate) then showTime end showTime_A
	,case when year(FDSellingdate)		= year(inpdate) and  month(FDSellingdate)		= month(inpdate) then FDSellingdate end FDSellingdate_A
	,case when year(FDSellingdate)		= year(inpdate) and  month(FDSellingdate)		= month(inpdate) then SignUp end SignUp_A
	,case when year(FDSellingdate)		= year(inpdate) and  month(FDSellingdate)		= month(inpdate) then STV end STV_A
	,*	
into #MDassigntoBDtrace_
from #tmp_ (nolock)


/*-----------------------------------------*/

IF OBJECT_ID('TempDb.dbo.#MDassigntoBDtrace_SUM_','U') IS NOT NULL
BEGIN
    DROP TABLE #MDassigntoBDtrace_SUM_;
END;



	select 媒體品牌, 媒體渠道
			, count (Agent_first) as 派發數
			, sum (case when ReservTime_A is not null then 1 else 0 end) as 預約數
			, sum (case when ShouldShowTime_A is not null then 1 else 0 end) as 應出席數
			, sum (case when ShowTime_A is not null then 1 else 0 end) as 出席數
			, ISNULL(sum (SignUp_A),0) as 成交數
			, ISNULL(sum (STV_A),0) as STV
			, case when  sum (SignUp_A) =0 then 0 else ISNULL(sum (STV_A)/sum (SignUp_A),0) end as ATS
			, ISNULL(sum (SignUp_A)*1.0 / sum (case when assignDate_first is not null then 1 else 0 end), 0)as 轉換率
			, case when  sum (SignUp_A) =0 then 0 else sum (Signupterm)*1.0 / sum (SignUp_A) end as 成交週期
			, case when count (Agent_first)=0 then 0 else sum(STV_A)*1.0/count (Agent_first) end as STV_perassign
	into #MDassigntoBDtrace_SUM_
	from #MDassigntoBDtrace_
	--where leadbrandid in (1,3) and inpdate <@sdate
	where brandid in (1,3) and inpdate <@sdate
	group by 媒體品牌, 媒體渠道



;

IF OBJECT_ID('TempDb.dbo.#final1_','U') IS NOT NULL
BEGIN
    DROP TABLE #final1_;
END;


select 

 deptshort_first as deptshort_first_last
 ,ismgm
, count( 1 )as 派發數_last
, sum( case when ReservTime is null then 0 else 1 end)as 預約數_last
, sum( case when ShouldShowTime is null then 0 else 1 end)as 應出席數_last
, sum( case when ShowTime is null then 0 else 1 end)as 出席數_last
, ISNULL (sum (a.STV),0) as STV_last
, ISNULL (sum (a.SignUp),0) as 成交數_last
, case when count( lead_sn ) = 0 then null else ISNULL (sum (a.SignUp),0)*1.0 / ISNULL (count( lead_sn ),0) end  as '轉換率'
, sum(STV_perassign) as 預估STV_last

into #final1_

from #MDassigntoBDtrace_ a (nolock)

left join #MDassigntoBDtrace_SUM_ b
on a. 媒體品牌 = b.媒體品牌  and  a. 媒體渠道 = b.媒體渠道

where a. Inpdate >= @sdate and a. Inpdate < @edate
--and a. assignDate_first >= @sdate and a. assignDate_first < @edate
and year(Inpdate) = year(@ydate) and month(Inpdate) = month(@ydate) --當月名單
and deptshort_first in (select  distinct DeptTW from bi_edw.dbo.FactSalesInfoFromPS with(nolock) where LeaveDate is null and DeptTW like N'TBD_業務部_BD%' )
--and a. 媒體品牌+a. 媒體渠道 in 
--(
--select 媒體品牌+媒體渠道
--from #MDassigntoBDtrace_SUM
--where (媒體渠道 ='Official Website'
--	or 媒體渠道 like N'網路%'
--	or 媒體渠道 like N'%tutorJr部落格%'
--	or 媒體渠道 in (N'講座和實體活動',N'地推活動') --offline
--	or 媒體渠道 in (N'社群媒體') 
--	--OR 媒體渠道 IN (N'客戶介紹', N'轉介紹') 
--	)
--)
group by 
 deptshort_first
 ,ismgm

IF OBJECT_ID('TempDb.dbo.#bind_','U') IS NOT NULL
BEGIN
    DROP TABLE #bind_;
END;

select 
		Deptshort_first
		,ismgm
		,count(isnull(BindLeadTime_new,bindleadtime)) as bind_cnt
		--,count(case when isnull(BindLeadTime_new,bindleadtime)<AssignDate_first then 1 else null end) as bind_cnt_before
		,count(case when isnull(BindLeadTime_new,bindleadtime)>dateadd(minute,1,AssignDate_first) and isnull(BindLeadTime_new,bindleadtime)<=dateadd(dd,30,AssignDate_first) then 1 else null end) as bind_cnt_after30
		--,count(case when isnull(BindLeadTime_new,bindleadtime)>AssignDate and isnull(BindLeadTime_new,bindleadtime)<=dateadd(dd,30,AssignDate) then 1 else null end) as bind_cnt_after30
into #bind_
from (select lead_sn,Deptshort_first,ismgm,AssignDate_first from #MDassigntoBDtrace_ where  Inpdate >=@sdate and Inpdate <@edate and year(Inpdate) = 2021 and month(Inpdate) = MONTH(@edate)) a
left join (select lead_sn,bindleadtime,BindLeadTime_new from  bi_edw.dbo.DimSocialLead  with(nolock)  where lead_sn in (select distinct lead_sn from #MDassigntoBDtrace_)) as dsl
	on a.lead_sn=dsl.lead_sn
group by 
		Deptshort_first
		,ismgm

		IF OBJECT_ID('TempDb.dbo.#final_last','U') IS NOT NULL
BEGIN
    DROP TABLE #final_last;
END;

select 
	#final1_.Deptshort_first_last
	, #final1_.ismgm
	,派發數_last
	,預約數_last
	,應出席數_last
	,出席數_last
	,STV_last
	,成交數_last
	,#bind_.bind_cnt as bind_cnt_last
	,預估STV_last
	,case when 派發數_last=0 then 0 else 預約數_last*1.0/派發數_last end as 預約率_last
	,case when 應出席數_last=0 then 0 else 出席數_last*1.0/應出席數_last end as 出席率_last
	,case when 出席數_last=0 then 0 else 成交數_last*1.0/出席數_last end as 成交率_last
	,case when 派發數_last=0 then 0 else 預約數_last*1.0/派發數_last end as 綁定率_last
	,case when 預估STV_last=0 then 0 else STV_last*1.0/預估STV_last end as 達成率_last
into #final_last
from #final1_
left join #bind_ on #final1_.deptshort_first_last=#bind_.Deptshort_first and #final1_.ismgm = #bind_. ismgm 
where #final1_.ismgm=@mgm
order by 2,1	;

		IF OBJECT_ID('TempDb.dbo.#final_this_sum','U') IS NOT NULL
BEGIN
    DROP TABLE #final_this_sum;
END;
select 
	ismgm
	,case when SUM(派發數)=0 then 0 else SUM(預約數)*1.0/SUM(派發數) end as 預約率_sum
	,case when SUM(應出席數)=0 then 0 else SUM(出席數)*1.0/SUM(應出席數) end as 出席率_sum
	,case when SUM(出席數)=0 then 0 else SUM(成交數)*1.0/SUM(出席數) end as 成交率_sum
	,case when SUM(派發數)=0 then 0 else SUM(預約數)*1.0/SUM(派發數) end as 綁定率_sum
	,case when SUM(預估STV)=0 then 0 else SUM(STV)*1.0/SUM(預估STV) end as 達成率_sum
	,case when COUNT(1) =0 then 0 else SUM(派發數)/COUNT(1) end as 派發數_sum
into #final_this_sum
from #final_this
group by ismgm

		IF OBJECT_ID('TempDb.dbo.#final_this_mm','U') IS NOT NULL
BEGIN
    DROP TABLE #final_this_mm;
END;
select 
	ismgm
	,MIN(預約率) as 預約率_min
	,MIN(出席率) as 出席率_min
	,MIN(成交率) as 成交率_min
	,MIN(綁定率) as 綁定率_min
	,MIN(達成率) as 達成率_min
	,max(預約率) as 預約率_max
	,max(出席率) as 出席率_max
	,max(成交率) as 成交率_max
	,max(綁定率) as 綁定率_max
	,max(達成率) as 達成率_max
into #final_this_mm
from #final_this
group by ismgm

		IF OBJECT_ID('TempDb.dbo.#final_last_sum','U') IS NOT NULL
BEGIN
    DROP TABLE #final_last_sum;
END;
select 
	ismgm
	,case when SUM(派發數_last)=0 then 0 else SUM(預約數_last)*1.0/SUM(派發數_last) end as 預約率_sum_last
	,case when SUM(應出席數_last)=0 then 0 else SUM(出席數_last)*1.0/SUM(應出席數_last) end as 出席率_sum_last
	,case when SUM(出席數_last)=0 then 0 else SUM(成交數_last)*1.0/SUM(出席數_last) end as 成交率_sum_last
	,case when SUM(派發數_last)=0 then 0 else SUM(預約數_last)*1.0/SUM(派發數_last) end as 綁定率_sum_last
	,case when SUM(預估STV_last)=0 then 0 else SUM(STV_last)*1.0/SUM(預估STV_last) end as 達成率_sum_last
	,case when  COUNT(1) =0 then 0 else SUM(派發數_last)/COUNT(1) end as 派發數_sum_last
into #final_last_sum
from #final_last
group by ismgm

		IF OBJECT_ID('TempDb.dbo.#final_last_mm','U') IS NOT NULL
BEGIN
    DROP TABLE #final_last_mm;
END;
select 
	ismgm
	,MIN(預約率_last) as 預約率_last_min
	,MIN(出席率_last) as 出席率_last_min
	,MIN(成交率_last) as 成交率_last_min
	,MIN(綁定率_last) as 綁定率_last_min
	,MIN(達成率_last) as 達成率_last_min
	,max(預約率_last) as 預約率_last_max
	,max(出席率_last) as 出席率_last_max
	,max(成交率_last) as 成交率_last_max
	,max(綁定率_last) as 綁定率_last_max
	,max(達成率_last) as 達成率_last_max
into #final_last_mm
from #final_last
group by ismgm

select 
	#final_this.Deptshort_first,
	#final_this.預約率,
	#final_this.出席率,
	#final_this.成交率,
	#final_this.綁定率,
	#final_this.達成率,
	#final_this.派發數,
	#final_last.預約率_last,
	#final_last.出席率_last,
	#final_last.成交率_last,
	#final_last.綁定率_last,
	#final_last.達成率_last,
	#final_last.派發數_last,
	#final_this_mm.*,
	#final_last_mm.*

from #final_this 
left join #final_last on #final_last.deptshort_first_last=#final_this.Deptshort_first 
left join #final_this_mm on #final_this.ismgm=#final_this_mm.ismgm
left join #final_last_mm on #final_last.ismgm=#final_last_mm.ismgm

union 

select 
	'mean' as Deptshort_first,
	#final_this_sum.預約率_sum,
	#final_this_sum.出席率_sum,
	#final_this_sum.成交率_sum,
	#final_this_sum.綁定率_sum,
	#final_this_sum.達成率_sum,
	#final_this_sum.派發數_sum,
	#final_last_sum.預約率_sum_last,
	#final_last_sum.出席率_sum_last,
	#final_last_sum.成交率_sum_last,
	#final_last_sum.綁定率_sum_last,
	#final_last_sum.達成率_sum_last,
	#final_last_sum.派發數_sum_last,
	#final_this_mm.*,
	#final_last_mm.*

from #final_this_sum 
left join #final_last_sum on #final_last_sum.ismgm=#final_this_sum.ismgm 
left join #final_this_mm on #final_this_sum.ismgm=#final_this_mm.ismgm
left join #final_last_mm on #final_last_sum.ismgm=#final_last_mm.ismgm
SET ANSI_WARNINGS On;