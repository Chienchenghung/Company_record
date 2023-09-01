with #lead as (

select isnull(b.lead_sn,a.lead_sn) as lead_sn, a.inpdate 
from bi_edw.FactMDInputLeadListClass as a with(nolock)
left join bi_edw.DimSocialLead as b on a.SocialLeadId=b.SocialLeadId
inner join bi_edw.DimMediaDetail as c with(nolock) on a.market_media_key=c.MediaKey and c.L2_CNAME in (N'Google關鍵字_Branding',N'Google關鍵字', N'聯播網廣告')
where a.inpdate>=convert(date,DATEADD(DD,-90,GETDATE())) and a.lead_sn<>0
)
select 
	c.email
	,b.phone
	,a.lead_sn
	,a.Name
	,a.Time
	,a.Value
from (
	select isnull(b.lead_sn,a.lead_sn) as lead_sn,a.inpdate as Time,'NewLead' as Name,NULL as Value
	from bi_edw..FactMDInputLeadListClass as a with(nolock)
	left join bi_edw.DimSocialLead as b on a.SocialLeadId=b.SocialLeadId
	where a.inpdate>=convert(date,DATEADD(DD,-90,GETDATE()))
	---
	union all
	---
	select isnull(b.lead_sn,a.lead_sn) as lead_sn,AssignDate as Time,'Assign' as Name,NULL as Value
	from bi_edw..FactLeadAssign as a with(nolock)
	left join bi_edw.DimSocialLead as b on a.SocialLeadId_String=b.SocialLeadId
	where a.AssignDate>=convert(date,DATEADD(DD,-90,GETDATE())) and DevelopTypeID=1 --and a.lead_sn=40292907
	---
	union all
	---
	select lead_sn,reserved_time as Time,'Appointment' as Name,NULL as Value
	from bi_edw.uvFactDemo with(nolock)
	where reserved_time>=convert(date,DATEADD(DD,-90,GETDATE()))
	---
	union all
	---
	select lead_sn,DemoDT as Time,'Attendance' as Name,NULL as Value
	from bi_edw.uvFactDemo with(nolock)
	where DemoDT>=convert(date,DATEADD(DD,-90,GETDATE())) and ClientDemoResult=1
	---
	union all
	---
	select lead_sn,fdsellingDate as Time,'Signup' as Name,BI_STV as Value
	from bi_edw.FactSignUpContractView with(nolock)
	where fdsellingDate>=convert(date,DATEADD(DD,-90,GETDATE())) and BI_STV>0
) as a 
left join bi_edw..DimLeadBrandID as b on a.lead_sn=b.lead_sn and DBSource='muchnewdb'
left join BI_muchnewdb..lead_basic as c on a.lead_sn=c.sn and email not like '%@default.com'
where a.lead_sn in (select lead_sn from #lead)


