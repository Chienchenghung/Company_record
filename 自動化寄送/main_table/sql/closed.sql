DECLARE @ydate datetime, @sdate datetime, @edate datetime, @ndate datetime;


SET @ydate= dateadd(day, -1, GETDATE()) ; --玡ぱ
SET @edate= dateadd(day, 1, @ydate) ; --さぱ
SET @sdate= DATEADD(M,DATEDIFF(m,0,@ydate),0) ; --讽る1腹

-----

		IF OBJECT_ID('TempDb.dbo.#final_this','U') IS NOT NULL
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
        ,count(1) as 祇计
        ,count(case when Fast30ReservDate<@edate then 1 else Null end) as 箇计
    	,count(case when Fast30Sh_rec_time<@edate then 1 else Null end) as 莱畊计
    	,count(case when DemoTime<@edate then 1 else Null end) as 畊计
    	,count(case when b.fdsellingDate<@edate and DemoTime is not null then 1 else Null end) as Θユ计
    	,isnull(sum(case when b.fdsellingDate<@edate and DemoTime is not null then BI_STV else Null end),0) as STV
		,count(1) * 100 as 箇︳STV
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
--	isnull(a.brand,null) as 穨叭珇礟
	isnull(dept.DeptTW,null) as Deptshort_first
	,'closed' as closed
	 ,isnull(a.祇计,0) as 祇计
--	,isnull(bind.bind_cnt_before,0) as 祇玡竕﹚计
--	,isnull(bind.bind_cnt_after7,0) as 祇7らず硑Θ竕﹚计
--	,isnull(bind.bind_cnt_after30,0) as 祇30らず硑Θ竕﹚计

    ,isnull(a.箇计,0) as 箇计
	,isnull(a.莱畊计,0) as 莱畊计
    ,isnull(a.畊计,0) as 畊计
    ,isnull(a.STV,0) as STV
    ,isnull(a.Θユ计,0) as Θユ计
	,isnull(bind.bind_cnt,0) as 竕﹚计
	,isnull(a.箇︳STV,0) as 箇︳STV
	,case when 祇计=0 then 0 else 箇计*1.0/祇计 end as 箇瞯
	,case when 莱畊计=0 then 0 else 畊计*1.0/莱畊计 end as 畊瞯
	,case when 畊计=0 then 0 else Θユ计*1.0/畊计 end as Θユ瞯
	,case when 祇计=0 then 0 else 箇计*1.0/祇计 end as 竕﹚瞯
	,case when 箇︳STV=0 then 0 else STV*1.0/箇︳STV end as 笷Θ瞯
into #final_this
FROM TABLE_A A
left join bind on --a.brand=bind.brand and 
a.DescrShort=bind.DescrShort
left join (select  distinct DeptShort,DeptTW from bi_edw.FactSalesInfoFromPS with(nolock) where LeaveDate is null and (DeptTW like N'TBD_穨叭场_BD%' or DeptTW like N'穨叭场_DTS%') ) as dept on dept.DeptShort=A.DescrShort
where dept.DeptTW is not null
order by 1,2,3
;


SET @ydate= dateadd(m,-1,dateadd(day, -1, GETDATE())) ; --玡る玡ぱ
SET @edate= dateadd(day, 1, @ydate) ; --さぱ
SET @sdate= DATEADD(M,DATEDIFF(m,0,@ydate),0) ; --讽る1腹

		IF OBJECT_ID('TempDb.dbo.#final_last','U') IS NOT NULL
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
        ,count(1) as 祇计
        ,count(case when Fast30ReservDate<@edate then 1 else Null end) as 箇计
    	,count(case when Fast30Sh_rec_time<@edate then 1 else Null end) as 莱畊计
    	,count(case when DemoTime<@edate then 1 else Null end) as 畊计
    	,count(case when b.fdsellingDate<@edate and DemoTime is not null then 1 else Null end) as Θユ计
    	,isnull(sum(case when b.fdsellingDate<@edate and DemoTime is not null then BI_STV else Null end),0) as STV
		,count(1) * 100 as 箇︳STV
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
	,isnull(a.祇计,0) as 祇计_last
    ,isnull(a.箇计,0) as 箇计_last
	,isnull(a.莱畊计,0) as 莱畊计_last
    ,isnull(a.畊计,0) as 畊计_last
    ,isnull(a.STV,0) as STV_last
    ,isnull(a.Θユ计,0) as Θユ计_last
	,isnull(bind.bind_cnt,0) as 竕﹚计_last
	,isnull(a.箇︳STV,0) as 箇︳STV_last
	,case when 祇计=0 then 0 else 箇计*1.0/祇计 end as 箇瞯_last
	,case when 莱畊计=0 then 0 else 畊计*1.0/莱畊计 end as 畊瞯_last
	,case when 畊计=0 then 0 else Θユ计*1.0/畊计 end as Θユ瞯_last
	,case when 祇计=0 then 0 else 箇计*1.0/祇计 end as 竕﹚瞯_last
	,case when 箇︳STV=0 then 0 else STV*1.0/箇︳STV end as 笷Θ瞯_last
into #final_last
FROM TABLE_A A
left join bind on --a.brand=bind.brand and 
a.DescrShort=bind.DescrShort
left join (select  distinct DeptShort,DeptTW from bi_edw.FactSalesInfoFromPS with(nolock) where LeaveDate is null and (DeptTW like N'TBD_穨叭场_BD%' or DeptTW like N'穨叭场_DTS%')  ) as dept on dept.DeptShort=A.DescrShort
where dept.DeptTW is not null
order by 1,2,3


		IF OBJECT_ID('TempDb.dbo.#final_this_sum','U') IS NOT NULL
BEGIN
    DROP TABLE #final_this_sum;
END;
select 
	closed
	,case when SUM(祇计)=0 then 0 else SUM(箇计)*1.0/SUM(祇计) end as 箇瞯_sum
	,case when SUM(莱畊计)=0 then 0 else SUM(畊计)*1.0/SUM(莱畊计) end as 畊瞯_sum
	,case when SUM(畊计)=0 then 0 else SUM(Θユ计)*1.0/SUM(畊计) end as Θユ瞯_sum
	,case when SUM(祇计)=0 then 0 else SUM(箇计)*1.0/SUM(祇计) end as 竕﹚瞯_sum
	,case when SUM(箇︳STV)=0 then 0 else SUM(STV)*1.0/SUM(箇︳STV) end as 笷Θ瞯_sum
	,case when COUNT(1) =0 then 0 else SUM(祇计)/COUNT(1) end as 祇计_sum
into #final_this_sum
from #final_this
group by closed

		IF OBJECT_ID('TempDb.dbo.#final_this_mm','U') IS NOT NULL
BEGIN
    DROP TABLE #final_this_mm;
END;
select 
	closed
	,MIN(箇瞯) as 箇瞯_min
	,MIN(畊瞯) as 畊瞯_min
	,MIN(Θユ瞯) as Θユ瞯_min
	,MIN(竕﹚瞯) as 竕﹚瞯_min
	,MIN(笷Θ瞯) as 笷Θ瞯_min
	,max(箇瞯) as 箇瞯_max
	,max(畊瞯) as 畊瞯_max
	,max(Θユ瞯) as Θユ瞯_max
	,max(竕﹚瞯) as 竕﹚瞯_max
	,max(笷Θ瞯) as 笷Θ瞯_max
into #final_this_mm
from #final_this
group by closed

		IF OBJECT_ID('TempDb.dbo.#final_last_sum','U') IS NOT NULL
BEGIN
    DROP TABLE #final_last_sum;
END;
select 
	closed
	,case when SUM(祇计_last)=0 then 0 else SUM(箇计_last)*1.0/SUM(祇计_last) end as 箇瞯_sum_last
	,case when SUM(莱畊计_last)=0 then 0 else SUM(畊计_last)*1.0/SUM(莱畊计_last) end as 畊瞯_sum_last
	,case when SUM(畊计_last)=0 then 0 else SUM(Θユ计_last)*1.0/SUM(畊计_last) end as Θユ瞯_sum_last
	,case when SUM(祇计_last)=0 then 0 else SUM(箇计_last)*1.0/SUM(祇计_last) end as 竕﹚瞯_sum_last
	,case when SUM(箇︳STV_last)=0 then 0 else SUM(STV_last)*1.0/SUM(箇︳STV_last) end as 笷Θ瞯_sum_last
	,case when  COUNT(1) =0 then 0 else SUM(祇计_last)/COUNT(1) end as 祇计_sum_last
into #final_last_sum
from #final_last
group by closed

		IF OBJECT_ID('TempDb.dbo.#final_last_mm','U') IS NOT NULL
BEGIN
    DROP TABLE #final_last_mm;
END;
select 
	closed
	,MIN(箇瞯_last) as 箇瞯_last_min
	,MIN(畊瞯_last) as 畊瞯_last_min
	,MIN(Θユ瞯_last) as Θユ瞯_last_min
	,MIN(竕﹚瞯_last) as 竕﹚瞯_last_min
	,MIN(笷Θ瞯_last) as 笷Θ瞯_last_min
	,max(箇瞯_last) as 箇瞯_last_max
	,max(畊瞯_last) as 畊瞯_last_max
	,max(Θユ瞯_last) as Θユ瞯_last_max
	,max(竕﹚瞯_last) as 竕﹚瞯_last_max
	,max(笷Θ瞯_last) as 笷Θ瞯_last_max
into #final_last_mm
from #final_last
group by closed

select 
	#final_this.Deptshort_first,
	#final_this.箇瞯,
	#final_this.畊瞯,
	#final_this.Θユ瞯,
	#final_this.竕﹚瞯,
	#final_this.笷Θ瞯,
	#final_this.祇计,
	#final_last.箇瞯_last,
	#final_last.畊瞯_last,
	#final_last.Θユ瞯_last,
	#final_last.竕﹚瞯_last,
	#final_last.笷Θ瞯_last,
	#final_last.祇计_last,
	#final_this_mm.*,
	#final_last_mm.*

from #final_this 
left join #final_last on #final_last.deptshort_first_last=#final_this.Deptshort_first 
left join #final_this_mm on #final_this.closed=#final_this_mm.closed
left join #final_last_mm on #final_last.closed=#final_last_mm.closed

union 

select 
	'mean' as Deptshort_first,
	#final_this_sum.箇瞯_sum,
	#final_this_sum.畊瞯_sum,
	#final_this_sum.Θユ瞯_sum,
	#final_this_sum.竕﹚瞯_sum,
	#final_this_sum.笷Θ瞯_sum,
	#final_this_sum.祇计_sum,
	#final_last_sum.箇瞯_sum_last,
	#final_last_sum.畊瞯_sum_last,
	#final_last_sum.Θユ瞯_sum_last,
	#final_last_sum.竕﹚瞯_sum_last,
	#final_last_sum.笷Θ瞯_sum_last,
	#final_last_sum.祇计_sum_last,
	#final_this_mm.*,
	#final_last_mm.*

from #final_this_sum 
left join #final_last_sum on #final_last_sum.closed=#final_this_sum.closed 
left join #final_this_mm on #final_this_sum.closed=#final_this_mm.closed
left join #final_last_mm on #final_last_sum.closed=#final_last_mm.closed