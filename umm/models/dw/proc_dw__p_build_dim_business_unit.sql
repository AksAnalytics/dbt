CREATE OR REPLACE PROCEDURE dw.p_build_dim_business_unit(flag_reload integer)
 LANGUAGE plpgsql
AS $$
--call dw.p_build_dim_business_unit (1)
BEGIN
	
	/* check for full reload */
	if(flag_reload = 1)
	then
		delete from dw.dim_business_unit;
	end if;
	--TESTING
	--delete from dw.dim_business_unit;
	--call dw.p_build_dim_business_unit(1)
	--select count(*) from dw.dim_business_unit
	drop table if exists stage_dim_business_unit;
		create temporary table stage_dim_business_unit
		diststyle all
		as 
		select 
			'unknown' as bar_entity,
		    'unknown' as bar_entity_description,
		    'unknown' as geography,
		    'unknown' as region,
		    'unknown' as subregion,
		    cast('1900-01-01' as date) as start_date,
		    cast('9999-12-31' as date) as end_date,
		    cast('1900-01-01' as date) as audit_loadts
		union
		select 	distinct 
				ent.name as bar_entity,
				ent.description as bar_entity_description,
				ent.level4 as geography,
				ent.level5 as region,
				ent.level6 as subregion,
			    getdate() as start_date,
			    cast('9999-12-31' as date) as end_date,
			    getdate() as audit_loadts
		from 	ref_data.entity ent
		where  level4 = 'GTS_NA'
		union 
		select 
			'ADJ_RSA' as bar_entity,
		    'ADJ_RSA' as bar_entity_description,
		    'ADJ_RSA' as geography,
		    'ADJ_RSA' as region,
		    'ADJ_RSA' as subregion,
		    cast('1900-01-01' as date) as start_date,
		    cast('9999-12-31' as date) as end_date,
		    cast('1900-01-01' as date) as audit_loadts
	;
	drop table if exists stage_dim_business_unit_i;
	create temporary table stage_dim_business_unit_i
	diststyle all 
	as 
	Select s.bar_entity,
		  s.bar_entity_description,
		  s.geography,
		  s.region,
		  s.subregion,
		  s.start_date,
		  s.end_date,
		  s.audit_loadts
	from stage_dim_business_unit s 
	left join dw.dim_business_unit t on s.bar_entity = t.bar_entity
	where t.bar_entity is null; 
		
	insert into dw.dim_business_unit (
				bar_entity,
				bar_entity_description,
				geography,
				region,
				subregion,
				start_date,
				end_date,
				audit_loadts
		)
	Select *
	from stage_dim_business_unit_i
	;

	update dw.dim_business_unit 
		set bar_entity_description=s.bar_entity_description,
		  geography=s.geography,
		  region=s.region,
		  subregion=s.subregion,
		  start_date=s.start_date,
		  end_date=s.end_date,
		  audit_loadts=s.audit_loadts
	from  stage_dim_business_unit s   
	where dim_business_unit.bar_entity = s.bar_entity
	and (dim_business_unit.bar_entity_description!=s.bar_entity_description OR
		dim_business_unit.geography!=s.geography   OR 
		dim_business_unit.subregion!=s.subregion OR 
		dim_business_unit.region!=s.region
		)
;

	EXCEPTION
		when others then raise info 'exception occur while ingesting data in dim_business_unit';
END
$$
;