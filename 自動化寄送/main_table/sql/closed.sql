DECLARE @ydate datetime, @sdate datetime, @edate datetime, @ndate datetime;


SET @ydate= dateadd(day, -1, GETDATE()) ; --�e�@��
SET @edate= dateadd(day, 1, @ydate) ; --����
SET @sdate= DATEADD(M,DATEDIFF(m,0,@ydate),0) ; --���1��

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
        ,count(1) as ���o��
        ,count(case when Fast30ReservDate<@edate then 1 else Null end) as �w����
    	,count(case when Fast30Sh_rec_time<@edate then 1 else Null end) as ���X�u��
    	,count(case when DemoTime<@edate then 1 else Null end) as �X�u��
    	,count(case when b.fdsellingDate<@edate and DemoTime is not null then 1 else Null end) as �����
    	,isnull(sum(case when b.fdsellingDate<@edate and DemoTime is not null then BI_STV else Null end),0) as STV
		,count(1) * 100 as �w��STV
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
--	isnull(a.brand,null) as �~�ȫ~�P
	isnull(dept.DeptTW,null) as Deptshort_first
	,'closed' as closed
	 ,isnull(a.���o��,0) as ���o��
--	,isnull(bind.bind_cnt_before,0) as ���o�e�w�j�w��
--	,isnull(bind.bind_cnt_after7,0) as ���o��7�餺�y���j�w��
--	,isnull(bind.bind_cnt_after30,0) as ���o��30�餺�y���j�w��

    ,isnull(a.�w����,0) as �w����
	,isnull(a.���X�u��,0) as ���X�u��
    ,isnull(a.�X�u��,0) as �X�u��
    ,isnull(a.STV,0) as STV
    ,isnull(a.�����,0) as �����
	,isnull(bind.bind_cnt,0) as �j�w��
	,isnull(a.�w��STV,0) as �w��STV
	,case when ���o��=0 then 0 else �w����*1.0/���o�� end as �w���v
	,case when ���X�u��=0 then 0 else �X�u��*1.0/���X�u�� end as �X�u�v
	,case when �X�u��=0 then 0 else �����*1.0/�X�u�� end as ����v
	,case when ���o��=0 then 0 else �w����*1.0/���o�� end as �j�w�v
	,case when �w��STV=0 then 0 else STV*1.0/�w��STV end as �F���v
into #final_this
FROM TABLE_A A
left join bind on --a.brand=bind.brand and 
a.DescrShort=bind.DescrShort
left join (select  distinct DeptShort,DeptTW from bi_edw.FactSalesInfoFromPS with(nolock) where LeaveDate is null and (DeptTW like N'TBD_�~�ȳ�_BD%' or DeptTW like N'�~�ȳ�_DTS%') ) as dept on dept.DeptShort=A.DescrShort
where dept.DeptTW is not null
order by 1,2,3
;


SET @ydate= dateadd(m,-1,dateadd(day, -1, GETDATE())) ; --�e��e�@��
SET @edate= dateadd(day, 1, @ydate) ; --����
SET @sdate= DATEADD(M,DATEDIFF(m,0,@ydate),0) ; --���1��

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
        ,count(1) as ���o��
        ,count(case when Fast30ReservDate<@edate then 1 else Null end) as �w����
    	,count(case when Fast30Sh_rec_time<@edate then 1 else Null end) as ���X�u��
    	,count(case when DemoTime<@edate then 1 else Null end) as �X�u��
    	,count(case when b.fdsellingDate<@edate and DemoTime is not null then 1 else Null end) as �����
    	,isnull(sum(case when b.fdsellingDate<@edate and DemoTime is not null then BI_STV else Null end),0) as STV
		,count(1) * 100 as �w��STV
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
	,isnull(a.���o��,0) as ���o��_last
    ,isnull(a.�w����,0) as �w����_last
	,isnull(a.���X�u��,0) as ���X�u��_last
    ,isnull(a.�X�u��,0) as �X�u��_last
    ,isnull(a.STV,0) as STV_last
    ,isnull(a.�����,0) as �����_last
	,isnull(bind.bind_cnt,0) as �j�w��_last
	,isnull(a.�w��STV,0) as �w��STV_last
	,case when ���o��=0 then 0 else �w����*1.0/���o�� end as �w���v_last
	,case when ���X�u��=0 then 0 else �X�u��*1.0/���X�u�� end as �X�u�v_last
	,case when �X�u��=0 then 0 else �����*1.0/�X�u�� end as ����v_last
	,case when ���o��=0 then 0 else �w����*1.0/���o�� end as �j�w�v_last
	,case when �w��STV=0 then 0 else STV*1.0/�w��STV end as �F���v_last
into #final_last
FROM TABLE_A A
left join bind on --a.brand=bind.brand and 
a.DescrShort=bind.DescrShort
left join (select  distinct DeptShort,DeptTW from bi_edw.FactSalesInfoFromPS with(nolock) where LeaveDate is null and (DeptTW like N'TBD_�~�ȳ�_BD%' or DeptTW like N'�~�ȳ�_DTS%')  ) as dept on dept.DeptShort=A.DescrShort
where dept.DeptTW is not null
order by 1,2,3


		IF OBJECT_ID('TempDb.dbo.#final_this_sum','U') IS NOT NULL
BEGIN
    DROP TABLE #final_this_sum;
END;
select 
	closed
	,case when SUM(���o��)=0 then 0 else SUM(�w����)*1.0/SUM(���o��) end as �w���v_sum
	,case when SUM(���X�u��)=0 then 0 else SUM(�X�u��)*1.0/SUM(���X�u��) end as �X�u�v_sum
	,case when SUM(�X�u��)=0 then 0 else SUM(�����)*1.0/SUM(�X�u��) end as ����v_sum
	,case when SUM(���o��)=0 then 0 else SUM(�w����)*1.0/SUM(���o��) end as �j�w�v_sum
	,case when SUM(�w��STV)=0 then 0 else SUM(STV)*1.0/SUM(�w��STV) end as �F���v_sum
	,case when COUNT(1) =0 then 0 else SUM(���o��)/COUNT(1) end as ���o��_sum
into #final_this_sum
from #final_this
group by closed

		IF OBJECT_ID('TempDb.dbo.#final_this_mm','U') IS NOT NULL
BEGIN
    DROP TABLE #final_this_mm;
END;
select 
	closed
	,MIN(�w���v) as �w���v_min
	,MIN(�X�u�v) as �X�u�v_min
	,MIN(����v) as ����v_min
	,MIN(�j�w�v) as �j�w�v_min
	,MIN(�F���v) as �F���v_min
	,max(�w���v) as �w���v_max
	,max(�X�u�v) as �X�u�v_max
	,max(����v) as ����v_max
	,max(�j�w�v) as �j�w�v_max
	,max(�F���v) as �F���v_max
into #final_this_mm
from #final_this
group by closed

		IF OBJECT_ID('TempDb.dbo.#final_last_sum','U') IS NOT NULL
BEGIN
    DROP TABLE #final_last_sum;
END;
select 
	closed
	,case when SUM(���o��_last)=0 then 0 else SUM(�w����_last)*1.0/SUM(���o��_last) end as �w���v_sum_last
	,case when SUM(���X�u��_last)=0 then 0 else SUM(�X�u��_last)*1.0/SUM(���X�u��_last) end as �X�u�v_sum_last
	,case when SUM(�X�u��_last)=0 then 0 else SUM(�����_last)*1.0/SUM(�X�u��_last) end as ����v_sum_last
	,case when SUM(���o��_last)=0 then 0 else SUM(�w����_last)*1.0/SUM(���o��_last) end as �j�w�v_sum_last
	,case when SUM(�w��STV_last)=0 then 0 else SUM(STV_last)*1.0/SUM(�w��STV_last) end as �F���v_sum_last
	,case when  COUNT(1) =0 then 0 else SUM(���o��_last)/COUNT(1) end as ���o��_sum_last
into #final_last_sum
from #final_last
group by closed

		IF OBJECT_ID('TempDb.dbo.#final_last_mm','U') IS NOT NULL
BEGIN
    DROP TABLE #final_last_mm;
END;
select 
	closed
	,MIN(�w���v_last) as �w���v_last_min
	,MIN(�X�u�v_last) as �X�u�v_last_min
	,MIN(����v_last) as ����v_last_min
	,MIN(�j�w�v_last) as �j�w�v_last_min
	,MIN(�F���v_last) as �F���v_last_min
	,max(�w���v_last) as �w���v_last_max
	,max(�X�u�v_last) as �X�u�v_last_max
	,max(����v_last) as ����v_last_max
	,max(�j�w�v_last) as �j�w�v_last_max
	,max(�F���v_last) as �F���v_last_max
into #final_last_mm
from #final_last
group by closed

select 
	#final_this.Deptshort_first,
	#final_this.�w���v,
	#final_this.�X�u�v,
	#final_this.����v,
	#final_this.�j�w�v,
	#final_this.�F���v,
	#final_this.���o��,
	#final_last.�w���v_last,
	#final_last.�X�u�v_last,
	#final_last.����v_last,
	#final_last.�j�w�v_last,
	#final_last.�F���v_last,
	#final_last.���o��_last,
	#final_this_mm.*,
	#final_last_mm.*

from #final_this 
left join #final_last on #final_last.deptshort_first_last=#final_this.Deptshort_first 
left join #final_this_mm on #final_this.closed=#final_this_mm.closed
left join #final_last_mm on #final_last.closed=#final_last_mm.closed

union 

select 
	'mean' as Deptshort_first,
	#final_this_sum.�w���v_sum,
	#final_this_sum.�X�u�v_sum,
	#final_this_sum.����v_sum,
	#final_this_sum.�j�w�v_sum,
	#final_this_sum.�F���v_sum,
	#final_this_sum.���o��_sum,
	#final_last_sum.�w���v_sum_last,
	#final_last_sum.�X�u�v_sum_last,
	#final_last_sum.����v_sum_last,
	#final_last_sum.�j�w�v_sum_last,
	#final_last_sum.�F���v_sum_last,
	#final_last_sum.���o��_sum_last,
	#final_this_mm.*,
	#final_last_mm.*

from #final_this_sum 
left join #final_last_sum on #final_last_sum.closed=#final_this_sum.closed 
left join #final_this_mm on #final_this_sum.closed=#final_this_mm.closed
left join #final_last_mm on #final_last_sum.closed=#final_last_mm.closed