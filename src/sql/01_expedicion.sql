/*
Expedition Query (Summary dataset)
This query uses the Expedition result (Summary dataset) to know if some indicators were fulfilled.

Notes:
1. The comments must be inside / *  * /, as the interpreter will receive only one line string - - comments will lead to an error because
the remaining query would be comented.
2. Every string between {{}} (used twice for python formatting) are variables that are used and replaced by the script. Replacing the values
inside with real data will make the script not working as desired.
3. Executing this query will return an error because of the variables detailed above.
4. The name of the query is used by the script to save a record inside Dataset Queries History with it. It is not used more than a
readable way to identify the query.
*/
with po_ca as(
	/*
	Get PC_CA from the join between POE and POE for the given contract ID.
	The result includes a range of dates with the operational plan for each day,
	including if the plan has exceptions for only some days.
	*/
	select
		poe.id_contrato,
		poe.fe_fecha,
		poe.id_po,
		poe.id_it,
		poe.id_pc,
		poe.id_ve
	from (
		select
			id_contrato,
			fecha as fe_fecha,
			id_po,
			id_it,
			id_pc,
			id_ve
			from {poe_dataset}
		where id_contrato = '{contract_id}'
			and fecha between '{start_date}' and '{end_date}'
	) poe
	union
	select
		po.id_contrato,
		po.fe_fecha,
		po.id_po,
		po.id_it,
		po.id_pc,
		po.id_ve
	from (
		select po.id_contrato,
			fechas.fe_fecha,
			po.id_po,
			po.id_it,
			po.id_pc,
			po.id_ve
		from {po_dataset} as po
		inner join (select dd.fe_fecha::date from generate_series( '{start_date}' , '{end_date}' , '1 day'::interval) dd(fe_fecha)) as fechas
			on fechas.fe_fecha between po.fecha_inicio and po.fecha_fin
		where po.id_contrato = '{contract_id}'
	) po
	left join (
		select
			id_contrato,
			fecha as fe_fecha,
			id_po,
			id_it,
			id_pc,
			id_ve
			from {poe_dataset}
		where id_contrato = '{contract_id}'
			and fecha between '{start_date}'
			and '{end_date}'
	) poe
		on poe.id_contrato = po.id_contrato
		and poe.fe_fecha = po.fe_fecha
		where poe.id_contrato is null
),

pc_ca as (
	/*Get PC_CA from the join between PO_CA and PC*/
	select distinct on (po_ca.id_pc, po_ca.fe_fecha, pc.id_servicio, pc.id_sentido, pc.n_pc)
		po_ca.id_contrato,
		po_ca.fe_fecha,
		po_ca.id_pc,
		pc.id_servicio,
		pc.id_sentido,
		pc.n_pc,
		pc.radio,
		ST_MakePoint(pc.lon::numeric, pc.lat::numeric) as point_pc
	from po_ca
	left join {pc_dataset} as pc
		on pc.id_contrato = po_ca.id_contrato
		and pc.id_pc = po_ca.id_pc
	order by po_ca.id_pc, po_ca.fe_fecha, pc.id_servicio, pc.id_sentido, pc.n_pc
),

expedicion_pc as (
/*Cleaning data 2*/
select
    fe_fecha,
    id_pc,
    id_vehiculo,
    id_servicio,
    id_sentido,
    id_expedicion,
    n_pc,
    max_pc,
    id_gps,
    fechahora_local,
    basura
from (
	/*Cleaning data and complete the remaining data with the GPS dataset data.*/
	select distinct on (eid.id_pc, eid.id_vehiculo, eid.id_servicio, eid.id_sentido, eid.id_expedicion, eid.n_pc)    
		eid.fe_fecha,
		eid.id_pc,
		eid.id_vehiculo,
        eid.id_servicio,
        eid.id_sentido,
        eid.id_expedicion,
        eid.n_pc,
        pc_data.max_pc,
        case
            when (max(n_pc) over (partition by eid.id_expedicion order by eid.id_gps rows between unbounded preceding and current row)) = pc_data.max_pc and eid.n_pc < pc_data.max_pc
            then 1 else 0
        end as post_max_pc,
        eid.id_gps,
        gps.fechahora_local,
        eid.basura
    from (
/*4.2 EXPEDICION_ID: START*/
	/*Get Expedition data and adding a unique ID for the expedition event*/
		select
			fe_fecha,
			id_pc,
			id_vehiculo,
			id_servicio,
			id_sentido,
			n_pc,
			id_gps,
			inicio_recorrido,
			sum(inicio_recorrido) over (order by id_pc, id_vehiculo, id_servicio, id_sentido, id_gps rows between unbounded preceding and current row) as id_expedicion,
			basura
		from (
/*4.1 EVENTO_RECORRIDO: START*/
			/*Get the start of the Event's path.*/
			select
				fe_fecha,
				id_pc,
				id_vehiculo,
				id_servicio,
				id_sentido,
				n_pc,
				id_gps,
				case
					when n_pc = 1 then 1
					when id_pc != lag(id_pc,1) over(order by id_pc, id_vehiculo, id_servicio, id_sentido, id_gps) then 1
					when id_vehiculo != lag(id_vehiculo,1) over(order by id_pc, id_vehiculo, id_servicio, id_sentido, id_gps) then 1
					when id_servicio != lag(id_servicio,1) over(order by id_pc, id_vehiculo, id_servicio, id_sentido, id_gps) then 1
					when id_sentido != lag(id_sentido,1) over(order by id_pc, id_vehiculo, id_servicio, id_sentido, id_gps) then 1
					else 0
				end as inicio_recorrido,
				case
					when n_pc = 1 then 0 when id_pc != lag(id_pc,1) over(order by id_pc, id_vehiculo, id_servicio, id_sentido, id_gps) then 1
					when id_vehiculo != lag(id_vehiculo,1) over(order by id_pc, id_vehiculo, id_servicio, id_sentido, id_gps) then 1
					when id_servicio != lag(id_servicio,1) over(order by id_pc, id_vehiculo, id_servicio, id_sentido, id_gps) then 1
					when id_sentido != lag(id_sentido,1) over(order by id_pc, id_vehiculo, id_servicio, id_sentido, id_gps) then 1
					else 0
				end as basura
			from (
/*3.2 EVENTO_VALIDO:START*/
				/*Check the Event PC validity.*/
				select
					fe_fecha,
					id_pc,
					id_vehiculo,
					id_servicio,
					id_sentido,
					n_pc,
					id_gps,
					case
						when n_pc != 1 or n_pc = lead(n_pc,1) over(partition by id_pc, id_vehiculo, id_servicio, id_sentido order by id_pc, id_vehiculo, id_servicio, id_gps) then false
						else true
					end as inicio_valido,
					case
						when n_pc = 1 or n_pc = lag(n_pc,1) over(partition by id_pc, id_vehiculo, id_servicio, id_sentido order by id_pc, id_vehiculo, id_servicio, id_gps) then false
						else true
					end as control_valido
				from (
/*3.1.2 EVENTO_PC: START*/
					/*Event PC which determines event state for the given PC based on the GPS information.*/
					select
						fe_fecha,
						id_pc,
						id_vehiculo,
						id_servicio,
						id_sentido,
						n_pc,
						id_gps,
						case
							when lag(id_gps,1) over(partition by id_pc, id_vehiculo, id_servicio, id_sentido, n_pc order by id_pc, id_vehiculo, id_servicio, id_sentido, n_pc, id_gps) is null
								then case
									when id_gps+1 < lead(id_gps,1) over(partition by id_pc, id_vehiculo, id_servicio, id_sentido, n_pc order by id_pc, id_vehiculo, id_servicio, id_sentido, n_pc, id_gps)
										then 11
									else 1
								end
							when lead(id_gps,1) over(partition by id_pc, id_vehiculo, id_servicio, id_sentido, n_pc order by id_pc, id_vehiculo, id_servicio, id_sentido, n_pc, id_gps) is null
								then case
									when id_gps-1 > lag(id_gps,1) over(partition by id_pc, id_vehiculo, id_servicio, id_sentido, n_pc order by id_pc, id_vehiculo, id_servicio, id_sentido, n_pc, id_gps)
										then 11
									else -1
								end
							when id_gps+1 < lead(id_gps,1) over(partition by id_pc, id_vehiculo, id_servicio, id_sentido, n_pc order by id_pc, id_vehiculo, id_servicio, id_sentido, n_pc, id_gps) and
								id_gps-1 > lag(id_gps,1) over(partition by id_pc, id_vehiculo, id_servicio, id_sentido, n_pc order by id_pc, id_vehiculo, id_servicio, id_sentido, n_pc, id_gps)
								then 11
							when id_gps+1 < lead(id_gps,1) over(partition by id_pc, id_vehiculo, id_servicio, id_sentido, n_pc order by id_pc, id_vehiculo, id_servicio, id_sentido, n_pc, id_gps)
								then -1
							when id_gps-1 > lag(id_gps,1) over(partition by id_pc, id_vehiculo, id_servicio, id_sentido, n_pc order by id_pc, id_vehiculo, id_servicio, id_sentido, n_pc, id_gps)
								then 1
							else 0
						end as estado_evento
					from (
/*3.1.1 EVENTO_PC0: START*/
						/*Get GPS for each day in PC_CA including if the GPS Point is within the PC_CA radius.*/
						select
							gps.fe_fecha,
							pc_ca.id_pc,
							gps.id_vehiculo,
							pc_ca.id_servicio,
							pc_ca.id_sentido,
							pc_ca.n_pc,
							gps.id_gps,
							ST_DWithin(gps.point_gps::geography, pc_ca.point_pc::geography, pc_ca.radio) as dentro_pc
						from (
/*2.1 GPS (1/2): START*/
							/*Get bus id related data from GPS dataset and transforming longitude and latitude to a Point.*/
							select
								row_number () over(partition by ppu order by ppu, gps_fecha_hora_chile) as id_gps,
								ppu as id_vehiculo,
								gps_fecha_hora_chile::date as fe_fecha,
								ST_MakePoint(gps_longitud::numeric, gps_latitud::numeric) as point_gps
							from {gps_dataset}
							where ppu = '{bus_id}'
							order by ppu, gps_fecha_hora_chile
/*2.1 GPS (1/2): END*/
						) gps
						join pc_ca on
							gps.fe_fecha = pc_ca.fe_fecha and
							ST_DWithin(gps.point_gps::geography, pc_ca.point_pc::geography, {max_radius})/*sacar valor de pc_ca (ojo que puede pegar en codigo python)*/
/*3.1.1 EVENTO_PC0: END*/
					) evento_pc0
					where dentro_pc
					order by id_pc, id_vehiculo, id_servicio, id_sentido, n_pc, id_gps
/*3.1.2 EVENTO_PC: END*/
				) evento_pc
				where (n_pc = 1 and (estado_evento = -1 or estado_evento = 11)) or
					(n_pc != 1 and (estado_evento = 1 or estado_evento = 11))
				order by id_pc, id_vehiculo, id_servicio, id_sentido, id_gps
/*3.2 EVENTO_VALIDO: END*/
			) evento_valido
			where
				inicio_valido or
				control_valido
			order by
				id_pc,
				id_vehiculo,
				id_servicio,
				id_sentido,
				id_gps
/*4.1 EVENTO RECORRIDO: END*/
		) evento_recorrido
		order by id_pc, id_vehiculo, id_servicio, id_sentido, id_gps
/*4.2 EXPEDICION_ID: END*/
	) eid
	left join (
		/*2.1 GPS (2/2): START*/
		select
			row_number () over(partition by ppu order by ppu, gps_fecha_hora_chile) as id_gps,
			ppu as id_vehiculo,
			gps_fecha_hora_chile as fechahora_local
		from {gps_dataset}
		where ppu = '{bus_id}'
		order by gps_fecha_hora_chile
		/*2.1 GPS (2/2): END*/
	) gps
	on eid.id_vehiculo = gps.id_vehiculo
	and eid.id_gps = gps.id_gps
	left join (
		/*PC_DATA: START*/
		select id_pc, id_servicio, id_sentido, max (n_pc) as max_pc
		from pc_ca
		group by id_pc, id_servicio, id_sentido
		order by id_pc, id_servicio, id_sentido
		/*PC_DATA: START*/
	) as pc_data on
		eid.id_pc = pc_data.id_pc and
		eid.id_servicio = pc_data.id_servicio and
		eid.id_sentido = pc_data.id_sentido
	order by eid.id_pc, eid.id_vehiculo, eid.id_servicio, eid.id_sentido, eid.id_expedicion, eid.n_pc, eid.id_gps
) epc
where
    post_max_pc = 0
),

expedicion as (
    select
        '{dataset_name}' as dataset_name,
        '{gps_dataset}' as dataset_table_name,
        '{contract_id}' as id_contrato,
        id_expedicion,
        id_pc,
        id_vehiculo,
        id_servicio,
        id_sentido,
        hh_inicio,
        hh_fin,
        kpi_cumplimiento_pc
    from (
        /*Cleaning output*/
        select
            distinct on (id_vehiculo, hh_fin)
            id_expedicion,
            id_pc,
            id_vehiculo,
            id_servicio,
            id_sentido,
            hh_inicio,
            hh_fin,
            kpi_cumplimiento_pc,
            traslapes,
            basura
        from (
            /*Cleaning output*/
            select
                distinct on (id_vehiculo, hh_inicio)
                id_expedicion,
                id_pc,
                id_vehiculo,
                id_servicio,
                id_sentido,
                hh_inicio,
                hh_fin,
                kpi_cumplimiento_pc,
                traslapes,
                basura
            from (
/*EXPEDICION1: START*/
                /*Summary of all data and checking if there are overlaps.*/
                select
                    e0.id_expedicion,
                    e0.id_pc,
                    e0.id_vehiculo,
                    e0.id_servicio,
                    e0.id_sentido,
                    e0.hh_inicio,
                    e0.hh_fin,
                    e0.kpi_cumplimiento_pc,
                    basura,
                    case
                        when e0.kpi_cumplimiento_pc != 1
                             then case
                                when e0.hh_inicio < lag(e0.hh_fin, 1) over (partition by e0.id_vehiculo order by e0.id_vehiculo, e0.hh_inicio, e0.kpi_cumplimiento_pc desc, e0.hh_fin desc) and
                                    lag(e0.kpi_cumplimiento_pc, 1) over (partition by e0.id_vehiculo order by e0.id_vehiculo, e0.hh_inicio, e0.kpi_cumplimiento_pc desc, e0.hh_fin desc) = 1
                                    then 1
                                when e0.hh_fin > lead(e0.hh_inicio, 1) over (partition by e0.id_vehiculo order by e0.id_vehiculo, e0.hh_inicio, e0.kpi_cumplimiento_pc desc, e0.hh_fin desc) and
                                    lead(e0.kpi_cumplimiento_pc, 1) over (partition by e0.id_vehiculo order by e0.id_vehiculo, e0.hh_inicio, e0.kpi_cumplimiento_pc desc, e0.hh_fin desc) =1
                                    then 1
                                else 0
                            end
                        else 0
                    end as traslapes
                from (
/*EXPEDICION0: START*/
					/*Get All Expedition data and adding event hours with the fulfilment for the control point.*/
                    select
                        epc.id_expedicion,
                        epc.id_pc,
                        epc.id_vehiculo,
                        epc.id_servicio,
                        epc.id_sentido,
                        min(fechahora_local) as hh_inicio,
                        max(fechahora_local) as hh_fin,
                        count(id_vehiculo)::decimal/max(max_pc) as kpi_cumplimiento_pc,
                        case
                            when sum(basura) > 0
                                then 1
                            when count(id_vehiculo)::decimal/max(max_pc) < 0.60
                                then 1
                            when date_part('hour', max(fechahora_local) - min(fechahora_local)) * 3600 +
                                 date_part('minute', max(fechahora_local) - min(fechahora_local)) * 60 +
                                 date_part('second', max(fechahora_local) - min(fechahora_local)) >
                                 1.5*max(t_max.t_max)*60
                                then 1
                            else 0
                        end as basura,
                        1 as check_traslape_salida
                    from
                        expedicion_pc as epc
                    left join (
/*T_MAX: START*/
                        select
                            id_it.id_servicio,
                            id_it.id_sentido,
                            it_pc.id_pc,
                            max(id_it.t_max) as t_max
                        from {it_dataset} as id_it
                        inner join (
                            select distinct
                                id_it,
                                id_pc
                            from po_ca
                        ) as it_pc on
                            it_pc.id_it = it_pc.id_it
                        where
                            id_contrato = '{contract_id}'
                        group by
                            id_it.id_servicio,
                            id_sentido,
                            it_pc.id_pc
                        order by
                            id_it.id_servicio,
                            id_it.id_sentido,
                            it_pc.id_pc
/*T_MAX: END*/
                    ) as t_max on
                        t_max.id_servicio = epc.id_servicio and
                        t_max.id_sentido = epc.id_sentido and
                        t_max.id_pc = epc.id_pc
                    group by
                        epc.id_pc,
                        id_vehiculo,
                        epc.id_servicio,
                        epc.id_sentido,
                        id_expedicion
                    order by
                        epc.id_pc,
                        id_vehiculo,
                        epc.id_servicio,
                        epc.id_sentido,
                        id_expedicion
/*EXPEDICION0: END*/
                ) e0
/*EXPEDICION1: END*/
            ) e1
            order by id_vehiculo, hh_inicio, kpi_cumplimiento_pc desc, hh_fin desc
        ) e2
        order by id_vehiculo, hh_fin, kpi_cumplimiento_pc desc, hh_inicio
    ) e3
    where traslapes = 0 and basura = 0
),
/*
Insert description of query
*/
insert_eventos_pasada as (
insert into dataset_241052 (
	dataset_name,
    dataset_table_name,
    id_contrato,
	id_vehiculo,
    id_expedicion,
    id_pc,
    id_servicio,
    id_sentido,
    hh_inicio,
    n_pc,
    evento_pasada,
    hh_pasada
)
    select
        null '{dataset_name}' as dataset_name,
        null '{gps_dataset}' as dataset_table_name,
        null '{contract_id}' as id_contrato,
        e.id_vehiculo,
        e.id_expedicion,
        e.id_pc,
        e.id_servicio,
        e.id_sentido,
        e.hh_inicio,
        pc_ca.n_pc,
        case
            when epc.n_pc > 0
                then 1
            else 0
        end as evento_pasada,
        epc.fechahora_local as hh_pasada
    from expedicion as e
    left join (
        select distinct
                id_pc,
                id_servicio,
                id_sentido,
                n_pc
            from
                pc_ca
    ) as pc_ca on
        e.id_pc = pc_ca.id_pc and
        e.id_servicio = pc_ca.id_servicio and
        e.id_sentido = pc_ca.id_sentido
    left join expedicion_pc as epc on
        e.id_vehiculo = epc.id_vehiculo and
        e.id_expedicion = epc.id_expedicion and
        pc_ca.n_pc = epc.n_pc
    order by e.id_expedicion, pc_ca.n_pc

)

/*
Insert all expedition data into Summary table for the given GPS dataset contract id. A difference from the Indicators query, here nested
queries are used because of performance issues due to the big GPS datasets can lead to long query run time. Also, the use of indexes
work better with nested queries than CTE for this particular case.
*/
INSERT INTO {summary_dataset} (
    dataset_name,
    dataset_table_name,
    id_contrato,
    id_expedicion,
    id_pc,
    id_vehiculo,
    id_servicio,
    id_sentido,
    hh_inicio,
    hh_fin,
    kpi_cumplimiento_pc
)
select
    dataset_name,
    dataset_table_name,
    id_contrato,
    id_expedicion,
    id_pc,
    id_vehiculo,
    id_servicio,
    id_sentido,
    hh_inicio,
    hh_fin,
    kpi_cumplimiento_pc
from expedicion



