declare @s_date date 						
declare @e_date date 						
declare @edate date = '2023-05-01';--預設今日						
declare @sdate date = '2022-12-01';--預設當月一號						
			
with	#media as (					
						
		select 				
			case when MD_ProductType='DSNY' then 7 when MD_ProductType='PG' then 9 when MD_ProductType='SA' then 11 else A.BrandId end Brandid			
			,A.L2_SN			
			,isnull(B.CATE_SN,0) as CATE_SN			
			,isnull(B.CATE_CNAME,'others') as CATE_CNAME			
			,A.MediaKey			
			,A.market_media_sn			
			,isnull(B.Category,'7-Other')  as Category			
			,isnull(b.cost_flag,'non-cost') as cost_flag			
		from DimMediaDetail AS A				
		LEFT JOIN DimMediaCategory AS B WITH(NOLOCK)				
		ON A.L2_SN=B.L2_SN				
	    where A.L1_sn<>119--排除香港 					
		and A.L2_SN not in (4322,1924)--排除代銷通路				
						
),	#cost as (					
	select 					
		year (inpdate) yr				
		,month(inpdate) mon				
		,B.BrandId				
		,B.CATE_SN				
		,B.Category				
		,sum(cost) as spending				
		,b.cost_flag				
	from FactMDMediaCost as a with(nolock)					
	INNER JOIN (select distinct L2_SN,CATE_SN,BrandId,Category,cost_flag from #media) AS B					
		ON A.L2_SN=B.L2_SN				
	where feeType=1  and  inpdate>=@sdate and inpdate<@edate					
			and B.BrandId in (1,3)			
	group by					
		year (inpdate) 				
		,month(inpdate)  				
		,B.BrandId				
		,B.BrandId				
		,B.CATE_SN				
		,B.Category				
		,b.cost_flag				
						
),	#lead as (					
		select 				
			YEAR(a.inpdate) as yr,			
			MONTH(a.inpdate) as mon,			
			b.BrandId,			
			b.CATE_SN,			
			b.CATE_CNAME,			
		    B.Category				
		    ,b.cost_flag				
	        ,COUNT(case when rn_inp=1 then 1 else null end) as lead					
			,COUNT(case when assigndate>=@sdate and assigndate<@edate and MONTH(assigndate)=month(inpdate) and rn_assign=1 then assigndate else null end) as assign			
			,COUNT(case when ReservTime>=@sdate and ReservTime<@edate and MONTH(ReservTime)=month(inpdate) and rn_demo=1 then 1 else null end) as app_cnt			
			,COUNT(case when shouldshowtime>=@sdate and shouldshowtime<@edate and MONTH(shouldshowtime)=month(inpdate) and rn_demo=1 then 1 else null end) as shatt_cnt			
			,COUNT(case when Showtime>=@sdate and Showtime<@edate and MONTH(Showtime)=month(inpdate) and rn_demo=1 then 1 else null end) as att_cnt			
			,SUM(case when a.fdsellingdate>=@sdate and a.fdsellingdate<@edate and MONTH(fdsellingdate)=month(inpdate) and rn_sign=1 and a.STVstream not in ('renewal','o2o','corporate') then signup else null end) as signup			
			,SUM(case when a.fdsellingdate>=@sdate and a.fdsellingdate<@edate and MONTH(fdsellingdate)=month(inpdate) and rn_sign=1 and a.STVstream not in ('renewal','o2o','corporate') then STV else null end) as STV			
		--into #lead				
		from FactMDFirstMediaMap as a				
		inner join #media as b 				
			on a.market_media_key=b.MediaKey 			
		where inpdate>=@sdate 				
				and inpdate<@edate 		
				--and isnull(assign_level,'') not in ('extra')		
				and class not in ( N'202205_少兒好友大募集',N'202205_成人好友大募集',N'202203_30秒快閃問卷')		
		group by 				
			YEAR(inpdate) ,			
			MONTH(inpdate) ,			
			b.BrandId,			
			b.CATE_SN,			
			b.CATE_CNAME,			
		    B.Category				
		    ,b.cost_flag				
)						
	, #total as (					
		select 				
			--'Total' as a,			
			--convert(varchar(10),@sdate) +' - ' +convert(varchar(10),dateadd(dd,-1,@edate)) as Period,			
			#lead.BrandId As BrandId,			
			'Total' as b,			
			99 as c ,			
			'Total' as d,			
			isnull(sum(#lead.lead),0) as [# of leads],			
			isnull(sum(#lead.assign),0) as [# of assign],			
			isnull(sum(#lead.app_cnt),0) as [# of app],			
			isnull(sum(#lead.shatt_cnt),0) as [# of shatt],			
			isnull(sum(#lead.att_cnt),0) as [# of att],			
			isnull(sum(#lead.signup),0) as [# of contract],			
			round(isnull(sum(#lead.STV),0),0) as [New Sales]			
		from #lead				
		group by 				
			#lead.BrandId			
)						
	, #subtotal as (					
		select 				
			--'Total' as a,			
			--convert(varchar(10),@sdate) +' - ' +convert(varchar(10),dateadd(dd,-1,@edate)) as Period,			
			#lead.BrandId As BrandId,			
			#lead.cost_flag as PayMedia,			
			98 as c ,			
			'SubTotal' as d,			
			isnull(sum(#lead.lead),0) as [# of leads],			
			isnull(sum(#lead.assign),0) as [# of assign],			
			isnull(sum(#lead.app_cnt),0) as [# of app],			
			isnull(sum(#lead.shatt_cnt),0) as [# of shatt],			
			isnull(sum(#lead.att_cnt),0) as [# of att],			
			isnull(sum(#lead.signup),0) as [# of contract],			
			round(isnull(sum(#lead.STV),0),0) as [New Sales]			
		from #lead				
		group by 				
			#lead.BrandId,			
			#lead.cost_flag			
)						
	, #target as (					
						
		select FinancialDate, 	    			
					case when salesbrand='TutorABC(TP)' then 1	
						when salesbrand='tutorJr' then 3
						when salesbrand='tutorming' then 5
				   end as ProBrandId,		
				   sum(STVGoal)*percentage as Target		
		from bi_twb.dbo.uvtwbsfactbodstvtarget as tgt with(nolock)				
		left join (				
			select f.ProBrandId,SUM(case when month(inpdate)=month(a.FDSellingDate) then STV end)/SUM(STV) as percentage			
			from FactMDFirstMediaMap as a with(nolock) 			
			left join FactSignUpContractView as f with(nolock) on a.contractsn=f.contractsn			
			where a.FDSellingDate>=@sdate and a.FDSellingDate<@edate and a.STVstream not in ('renewal','MGM','o2o','corporate') and rn_sign=1			
			group by f.ProBrandId			
		) as pr on	case when salesbrand='TutorABC(TP)' then 1			
						when salesbrand='tutorJr' then 33
						when salesbrand='tutorming' then 5
				   end=pr.ProBrandId		
		where 1=1				
			and targettype = 'STV'			
			and TypeNGroup in ('new','mgm')			
			and FinancialDate='2023-01-01'			
			and salesbrand in ('TutorMing','tutorJr','TutorABC(TP)')			
		group by 				
			FinancialDate,SalesBrand,percentage			
)						
						
		select 				
            --isnull(#lead.Category,'') as Category,						
			--convert(varchar(10),@sdate) +' - ' +convert(varchar(10),dateadd(dd,-1,@edate)) as Period,			
			#lead.BrandId As BrandId,			
			#lead.cost_flag as PayMedia,			
			#lead.cate_sn,			
			isnull(#lead.CATE_CNAME,'') as 'Media Channel',			
			isnull(sum(#lead.lead),0) as [# of leads],			
			isnull(sum(#lead.assign),0) as [# of assign],			
			isnull(sum(#lead.app_cnt),0) as [# of app],			
			isnull(sum(#lead.shatt_cnt),0) as [# of shatt],			
			isnull(sum(#lead.att_cnt),0) as [# of att],			
			isnull(sum(#lead.signup),0) as [# of contract],			
			round(isnull(sum(#lead.STV),0),0) as [New Sales]			
			--,#target.Target			
			into #result			
		from #lead				
		--left join #target on #lead.BrandId=#target.ProBrandId				
		group by 				
            --isnull(#lead.Category,''),						
			#lead.BrandId,			
			#lead.cate_sn,			
			isnull(#lead.CATE_CNAME,'')			
		    ,#lead.cost_flag				
			--,#target.Target			
						
		union all				
		select #total.*--,#target.Target 
		from #total				
		--left join #target on	#total.BrandId=#target.ProBrandId			
			   			
		union all				
		select #subtotal.*--,#target.Target 
		from #subtotal				
		--left join #target on	#subtotal.BrandId=#target.ProBrandId			
						
						
		order by 1,2,3				
						
		select * from #result where (brandid=1 and [media channel] in ('Social Media-Facebook',
'Social Media-Instgram',
'Social Media-TikTok',
'Social Media-LINE OA',
'KOL content authorization',
'SEO',
'Total',
'KOL Fee',
'KOL Ads Spending',
'KOL',
'Google Search',
'Google Display ads',
'Youtube',
'Yahoo Search',
'FB Ads_adGeek',
'FB Ads_JS',
'Blog (FB ads)',
'App',
'TikTok',
'foodpanda',
'invoice data_lifi',
'subtotal',
'eGentic',
'StarMedia',
'Barkley',
'104 App',
'subtotal',
'Synergy-KOC',
'iChannels',
'subtotal',
'IMC subtotal',
'Total'))
or (brandid=3 and [media channel] in (
'Social Media-Facebook',
'Social Media-Instgram',
'Social Media-LINE OA',
'KOL content authorization',
'SEO',
'subtotal',
'KOL Fee',
'KOL Ads Spending',
'KOL',
'Google Search',
'Google Display ads',
'Youtube',
'Yahoo Search',
'FB Ads_adGeek',
'FB Ads_JS',
'Blog (FB ads)',
'App',
'invoice data_invos',
'invoice data_lifi',
'subtotal',
'StarMedia',
'Barkley',
'subtotal',
'Synergy-KOC',
'iChannels',
'subtotal',
'Total'
))
or (brandid=5 and [media channel] in (
'Social Media-Facebook',
'Google Search'
))
or (brandid=7 and [media channel] in (
'KOL',
'FB Ads_adGeek',
'FB Ads_JS',
'Google Search',
'Google Display ads',
'subtotal',
'Total'
))
or (brandid=9 and [media channel] in (
'KOL',
'FB Ads_adGeek',
'FB Ads_JS',
'Blog (FB ads)',
'Google Search',
'Google Display ads',
'subtotal',
'IMC subtotal',
'Total'
))
or [media channel] in ('others',
'Synergy',
'Referral',
'Official Website',
--'Study Abroad',
'others-EDM',
'SubTotal',
'Total'
)