Select *
from (
	SELECT
		ps.managerEname
		, CONVERT(varchar(7),a.AssignDate,126) as YM
		--, COUNT(1) as assign
		--, COUNT(showTime) as Demo
		--, COUNT(a.FDSellingDate) as Signup
		--, SUM(a.STV) as STV
		--, COUNT(case when month(showTime)=MONTH(assigndate) and day(showtime)<day( '2023-04-24') then 1 end)*1.0 /COUNT(1) as rate
		, COUNT(case when month(a.fdsellingdate)=MONTH(assigndate) then signup end)*1.0 /COUNT(1) as rate
	FROM BI_EDW.dbo.FactMDFirstMediaMap AS a WITH(NOLOCK)
	--INNER JOIN BI_EDW.dbo.DimMediaDetail AS b WITH(NOLOCK)
	--	ON a.market_media_key=b.MediaKey 
	LEFT JOIN BI_EDW.dbo.FactSalesInfoFromPS AS ps WITH(NOLOCK)
		ON ps.IMSSalesSn=a.AssignSalesSn
	WHERE 1=1
		--AND a.Inpdate >= @_sdate --DSSR-1071
		--AND a.Inpdate < @_edate --DSSR-1071
		AND a.assignDate >= '2023-01-01'
		AND a.assignDate < '2023-06-01'
		AND DAY(a.AssignDate)<31
		AND a.rn_assign=1
		AND a.rn_sign=1 --DSSR-1071
		AND a.media_brandid=1
		AND ISNULL(a.DistributedType,0)<>18 --BIHD-2698
		and (ps.DeptShort like N'TBD.BD%' or  ps.DeptShort like N'BA%' )
		and a.assign_level not in ('mgm')
	GROUP BY
		ps.managerEname
		, CONVERT(varchar(7),a.AssignDate,126)
	UNION ALL
	SELECT
		'total'
		, CONVERT(varchar(7),a.AssignDate,126) as YM
		--, COUNT(1) as assign
		--, COUNT(showTime) as Demo
		--, COUNT(a.FDSellingDate) as Signup
		--, SUM(a.STV) as STV
		--, COUNT(case when month(showTime)=MONTH(assigndate) and day(showtime)<day( '2023-04-24') then 1 end)*1.0 /COUNT(1) as rate
		, COUNT(case when month(a.fdsellingdate)=MONTH(assigndate) then signup end)*1.0 /COUNT(1) as rate
	FROM BI_EDW.dbo.FactMDFirstMediaMap AS a WITH(NOLOCK)
	--INNER JOIN BI_EDW.dbo.DimMediaDetail AS b WITH(NOLOCK)
	--	ON a.market_media_key=b.MediaKey 
	LEFT JOIN BI_EDW.dbo.FactSalesInfoFromPS AS ps WITH(NOLOCK)
		ON ps.IMSSalesSn=a.AssignSalesSn
	WHERE 1=1
		--AND a.Inpdate >= @_sdate --DSSR-1071
		--AND a.Inpdate < @_edate --DSSR-1071
		AND a.assignDate >= '2023-01-01'
		AND a.assignDate < '2023-06-01'
		AND DAY(a.AssignDate)<31
		AND a.rn_assign=1
		AND a.rn_sign=1 --DSSR-1071
		AND a.media_brandid=1
		AND ISNULL(a.DistributedType,0)<>18 --BIHD-2698
		and (ps.DeptShort like N'TBD.BD%' or  ps.DeptShort like N'BA%' )
	GROUP BY CONVERT(varchar(7),a.AssignDate,126)

) as a 
pivot (
sum(rate) for YM in ([2023-01],[2023-02],[2023-03],[2023-04],[2023-05])
) as pt
order by 1