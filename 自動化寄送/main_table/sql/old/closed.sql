SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;
DECLARE @ydate datetime, @sdate datetime, @edate datetime, @ndate datetime;


SET @ydate= dateadd(day, -1, GETDATE()) ; --前一天
SET @edate= dateadd(day, 1, @ydate) ; --今天
SET @sdate= DATEADD(M,DATEDIFF(m,0,@ydate),0) ; --當月1號

-----

		IF OBJECT_ID('TempDb.#final_this','U') IS NOT NULL
BEGIN
    DROP TABLE #final_this;
END;

WITH
	assign as (
	select
		*	
	from bi_edw.FactLeadAssign a with(nolock)
	where assigndate>=@sdate
		and assigndate<@edate
		and DevelopTypeID=3	 
) , TABLE_A AS (
    select 
        --brand
		DescrShort
        ,count(1) as 派發數
        ,count(case when Fast30ReservDate<@edate then 1 else Null end) as 預約數
    	,count(case when Fast30Sh_rec_time<@edate then 1 else Null end) as 應出席數
    	,count(case when DemoTime<@edate then 1 else Null end) as 出席數
    	,count(case when b.fdsellingDate<@edate and DemoTime is not null then 1 else Null end) as 成交數
    	,isnull(sum(case when b.fdsellingDate<@edate and DemoTime is not null then BI_STV else Null end),0) as STV
		,count(1) * 100 as 預估STV
    from assign a
    left join bi_edw.FactSignUpContractView b with(nolock)
    on 1=1--a.contract_sn=b.contractsn
    and a.lead_sn=b.lead_sn
    and a.DemoTime<b.fdsellingDate
    and b.STVstream='closed'
    and BI_STV>0
    group by 
    	--brand
		DescrShort
), bind as (
	select 
		count(isnull(BindLeadTime_new,bindleadtime)) as bind_cnt
		--,count(case when isnull(BindLeadTime_new,bindleadtime)<AssignDate then 1 else null end) as bind_cnt_before
		--,count(case when isnull(BindLeadTime_new,bindleadtime)>AssignDate and isnull(BindLeadTime_new,bindleadtime)<=dateadd(dd,7,AssignDate) then 1 else null end) as bind_cnt_after7
		,count(case when isnull(BindLeadTime_new,bindleadtime)>AssignDate and isnull(BindLeadTime_new,bindleadtime)<=dateadd(dd,30,AssignDate) then 1 else null end) as bind_cnt_after30
		--,brand
		,DescrShort
	from assign
	left join bi_edw.DimSocialLead dsl with(nolock)
	on assign.lead_sn=dsl.lead_sn
	group by 
		--brand
		DescrShort
)



SELECT 
--	isnull(a.brand,null) as 業務品牌
	isnull(dept.DeptTW,null) as Deptshort_first
	,'closed' as closed
	 ,isnull(a.派發數,0) as 派發數
--	,isnull(bind.bind_cnt_before,0) as 派發前已綁定數
--	,isnull(bind.bind_cnt_after7,0) as 派發後7日內造成綁定數
--	,isnull(bind.bind_cnt_after30,0) as 派發後30日內造成綁定數

    ,isnull(a.預約數,0) as 預約數
	,isnull(a.應出席數,0) as 應出席數
    ,isnull(a.出席數,0) as 出席數
    ,isnull(a.STV,0) as STV
    ,isnull(a.成交數,0) as 成交數
	,isnull(bind.bind_cnt,0) as 綁定數
	,isnull(a.預估STV,0) as 預估STV
	,case when 派發數=0 then 0 else 預約數*1.0/派發數 end as 預約率
	,case when 應出席數=0 then 0 else 出席數*1.0/應出席數 end as 出席率
	,case when 出席數=0 then 0 else 成交數*1.0/出席數 end as 成交率
	,case when 派發數=0 then 0 else 預約數*1.0/派發數 end as 綁定率
	,case when 預估STV=0 then 0 else STV*1.0/預估STV end as 達成率
into #final_this
FROM TABLE_A A
left join bind on --a.brand=bind.brand and 
a.DescrShort=bind.DescrShort
left join (select  distinct DeptShort,DeptTW from bi_edw.FactSalesInfoFromPS with(nolock) where LeaveDate is null and DeptTW like N'TBD_業務部_BD%' ) as dept on dept.DeptShort=A.DescrShort
where dept.DeptTW is not null
order by 1,2,3
;


SET @ydate= dateadd(dd,-1,dateadd(m, datediff(m,0,getdate()),0)) ; --前一天
SET @edate= dateadd(day, 1, @ydate) ; --今天
SET @sdate= DATEADD(M,DATEDIFF(m,0,@ydate),0) ; --當月1號

		IF OBJECT_ID('TempDb.#final_last','U') IS NOT NULL
BEGIN
    DROP TABLE #final_last;
END;
-----
WITH
	assign as (
	select
		*	
	from bi_edw.FactLeadAssign a with(nolock)
	where assigndate>=@sdate
		and assigndate<@edate
		and DevelopTypeID=3	 
) , TABLE_A AS (
    select 
        --brand
		DescrShort
        ,count(1) as 派發數
        ,count(case when Fast30ReservDate<@edate then 1 else Null end) as 預約數
    	,count(case when Fast30Sh_rec_time<@edate then 1 else Null end) as 應出席數
    	,count(case when DemoTime<@edate then 1 else Null end) as 出席數
    	,count(case when b.fdsellingDate<@edate and DemoTime is not null then 1 else Null end) as 成交數
    	,isnull(sum(case when b.fdsellingDate<@edate and DemoTime is not null then BI_STV else Null end),0) as STV
		,count(1) * 100 as 預估STV
    from assign a
    left join bi_edw.FactSignUpContractView b with(nolock)
    on 1=1--a.contract_sn=b.contractsn
    and a.lead_sn=b.lead_sn
    and a.DemoTime<b.fdsellingDate
    and b.STVstream='closed'
    and BI_STV>0
    group by 
    	--brand
		DescrShort
), bind as (
	select 
		count(isnull(BindLeadTime_new,bindleadtime)) as bind_cnt
		--,count(case when isnull(BindLeadTime_new,bindleadtime)<AssignDate then 1 else null end) as bind_cnt_before
		--,count(case when isnull(BindLeadTime_new,bindleadtime)>AssignDate and isnull(BindLeadTime_new,bindleadtime)<=dateadd(dd,7,AssignDate) then 1 else null end) as bind_cnt_after7
		,count(case when isnull(BindLeadTime_new,bindleadtime)>AssignDate and isnull(BindLeadTime_new,bindleadtime)<=dateadd(dd,30,AssignDate) then 1 else null end) as bind_cnt_after30
		--,brand
		,DescrShort
	from assign
	left join bi_edw.DimSocialLead dsl with(nolock)
	on assign.lead_sn=dsl.lead_sn
	group by 
		--brand
		DescrShort
)



SELECT 
	isnull(dept.DeptTW,null) as Deptshort_first_last
	,'closed' as closed
	,isnull(a.派發數,0) as 派發數_last
    ,isnull(a.預約數,0) as 預約數_last
	,isnull(a.應出席數,0) as 應出席數_last
    ,isnull(a.出席數,0) as 出席數_last
    ,isnull(a.STV,0) as STV_last
    ,isnull(a.成交數,0) as 成交數_last
	,isnull(bind.bind_cnt,0) as 綁定數_last
	,isnull(a.預估STV,0) as 預估STV_last
	,case when 派發數=0 then 0 else 預約數*1.0/派發數 end as 預約率_last
	,case when 應出席數=0 then 0 else 出席數*1.0/應出席數 end as 出席率_last
	,case when 出席數=0 then 0 else 成交數*1.0/出席數 end as 成交率_last
	,case when 派發數=0 then 0 else 預約數*1.0/派發數 end as 綁定率_last
	,case when 預估STV=0 then 0 else STV*1.0/預估STV end as 達成率_last
into #final_last
FROM TABLE_A A
left join bind on --a.brand=bind.brand and 
a.DescrShort=bind.DescrShort
left join (select  distinct DeptShort,DeptTW from bi_edw.FactSalesInfoFromPS with(nolock) where LeaveDate is null and DeptTW like N'TBD_業務部_BD%' ) as dept on dept.DeptShort=A.DescrShort
where dept.DeptTW is not null
order by 1,2,3


		IF OBJECT_ID('TempDb.#final_this_sum','U') IS NOT NULL
BEGIN
    DROP TABLE #final_this_sum;
END;
select 
	closed
	,case when SUM(派發數)=0 then 0 else SUM(預約數)*1.0/SUM(派發數) end as 預約率_sum
	,case when SUM(應出席數)=0 then 0 else SUM(出席數)*1.0/SUM(應出席數) end as 出席率_sum
	,case when SUM(出席數)=0 then 0 else SUM(成交數)*1.0/SUM(出席數) end as 成交率_sum
	,case when SUM(派發數)=0 then 0 else SUM(預約數)*1.0/SUM(派發數) end as 綁定率_sum
	,case when SUM(預估STV)=0 then 0 else SUM(STV)*1.0/SUM(預估STV) end as 達成率_sum
	,case when COUNT(1) =0 then 0 else SUM(派發數)/COUNT(1) end as 派發數_sum
into #final_this_sum
from #final_this
group by closed

		IF OBJECT_ID('TempDb.#final_this_mm','U') IS NOT NULL
BEGIN
    DROP TABLE #final_this_mm;
END;
select 
	closed
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
group by closed

		IF OBJECT_ID('TempDb.#final_last_sum','U') IS NOT NULL
BEGIN
    DROP TABLE #final_last_sum;
END;
select 
	closed
	,case when SUM(派發數_last)=0 then 0 else SUM(預約數_last)*1.0/SUM(派發數_last) end as 預約率_sum_last
	,case when SUM(應出席數_last)=0 then 0 else SUM(出席數_last)*1.0/SUM(應出席數_last) end as 出席率_sum_last
	,case when SUM(出席數_last)=0 then 0 else SUM(成交數_last)*1.0/SUM(出席數_last) end as 成交率_sum_last
	,case when SUM(派發數_last)=0 then 0 else SUM(預約數_last)*1.0/SUM(派發數_last) end as 綁定率_sum_last
	,case when SUM(預估STV_last)=0 then 0 else SUM(STV_last)*1.0/SUM(預估STV_last) end as 達成率_sum_last
	,case when  COUNT(1) =0 then 0 else SUM(派發數_last)/COUNT(1) end as 派發數_sum_last
into #final_last_sum
from #final_last
group by closed

		IF OBJECT_ID('TempDb.#final_last_mm','U') IS NOT NULL
BEGIN
    DROP TABLE #final_last_mm;
END;
select 
	closed
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
group by closed

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
left join #final_this_mm on #final_this.closed=#final_this_mm.closed
left join #final_last_mm on #final_last.closed=#final_last_mm.closed

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
left join #final_last_sum on #final_last_sum.closed=#final_this_sum.closed 
left join #final_this_mm on #final_this_sum.closed=#final_this_mm.closed
left join #final_last_mm on #final_last_sum.closed=#final_last_mm.closed
SET ANSI_WARNINGS On;