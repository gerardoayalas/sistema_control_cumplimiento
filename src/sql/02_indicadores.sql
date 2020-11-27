/*
Indicators Query (Indicadores dataset)
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
/*Get all data from Expedition (Summary dataset) based on the contract ID and a range of dates.*/
with
expedicion as (
    select
    	id_contrato,
        id_pc,
        id_servicio,
        id_sentido,
    	id_expedicion,
        id_vehiculo,
        hh_inicio,
        hh_fin,
    	kpi_cumplimiento_pc
    from {summary_dataset}
    where id_contrato = '{contract_id}'
    	and hh_inicio::date between '{start_date}' and '{end_date}'
),
/*
Get PO dataset data between the range of dates passed and the given contract ID.
Creates a new table with every day between the range of dates including the data of the PO for each day.
*/
po as(
    select po.id_contrato,
        fechas.fe_fecha,
        po.id_po,
        po.id_it,
        po.id_pc,
        po.id_ve
    from {po_dataset} as po
    inner join (select dd.fe_fecha::date from generate_series('{start_date}', '{end_date}' , '1 day'::interval) dd(fe_fecha)) as fechas
    on fechas.fe_fecha between po.fecha_inicio and po.fecha_fin
    where po.id_contrato = '{contract_id}'
),
/*
Get POE dataset data between the range of dates passed and the given contract ID.
*/
poe as(
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
),
/*
Join PO and POE outputs to have the whole list of days with the PO data and the POE for the exception days.
*/
po_ca as(
    select
        poe.id_contrato,
        poe.fe_fecha,
        poe.id_po,
        poe.id_it,
        poe.id_pc,
        poe.id_ve
    from poe
    union
    select
        po.id_contrato,
        po.fe_fecha,
        po.id_po,
        po.id_it,
        po.id_pc,
        po.id_ve
    from po
    left join poe
        on poe.id_contrato = po.id_contrato
        and poe.fe_fecha = po.fe_fecha
        where poe.id_contrato is null
),
/*
Get IT data and join it with PO_CA data to have all itinerary data for every day as well.
*/
it_ca as(
    select
    	row_number() over(order by po_ca.id_contrato,po_ca.fe_fecha,po_ca.id_po,po_ca.id_it,it.id_servicio,it.id_sentido, it.n_pc, it.hh_control) as id_it_ca,
    	po_ca.id_contrato,
        po_ca.fe_fecha,
        po_ca.id_po,
        po_ca.id_it,
        po_ca.id_pc,
        it.id_servicio,
        it.id_sentido,
        it.n_pc,
        po_ca.fe_fecha + it.hh_control as hh_control,
        it.t_max,
        it.adelanto,
        it.atraso,
        case
            when date_part('dow', po_ca.fe_fecha) = 0 then it.dom
            when date_part('dow', po_ca.fe_fecha) = 1 then it.lun
            when date_part('dow', po_ca.fe_fecha) = 2 then it.mar
            when date_part('dow', po_ca.fe_fecha) = 3 then it.mie
            when date_part('dow', po_ca.fe_fecha) = 4 then it.jue
            when date_part('dow', po_ca.fe_fecha) = 5 then it.vie
            when date_part('dow', po_ca.fe_fecha) = 6 then it.sab
        end as operacion_programada
    from po_ca
    left join {it_dataset} as it
        on po_ca.id_contrato = it.id_contrato
        and po_ca.id_it = it.id_it
    where it.n_pc = 1
    order by po_ca.fe_fecha, po_ca.id_po, po_ca.id_it, it.id_servicio, it.id_sentido, it.hh_control
),
/*
Get VE data and join it with PO_CA data to have vehicles data based on the PO or POE values.
*/
ve_ca as(
	select
    	row_number() over(order by po_ca.fe_fecha, po_ca.id_po, po_ca.id_ve, ve.id_servicio, ve.id_vehiculo) as id_ve_ca,
        po_ca.fe_fecha,
        po_ca.id_po,
        po_ca.id_ve,
        po_ca.id_it,
        po_ca.id_pc,
    	ve.id_servicio,
    	ve.id_vehiculo
    from po_ca
    left join {ve_dataset} as ve
        on po_ca.id_contrato = ve.id_contrato
        and po_ca.id_ve = ve.id_ve
    order by po_ca.fe_fecha, po_ca.id_po, po_ca.id_ve, ve.id_servicio
),
/*
Get all indicators for the Expedition based on the Itinerary and Vehicle. It only takes into count the intersection between both,
it means that they fulfilled some conditions.
	1. Fullfilled with the control point.
    2. It is a programmed operation.
    3. It has an complete itinerary (start and end time).
    4. Expedition time is between the start and the end time of the itinerary.
*/
asignacion1a as(
    select distinct on(it_ca.id_it_ca)
    	it_ca.id_contrato,
        it_ca.id_it_ca,
        it_ca.fe_fecha,
        it_ca.id_it,
        it_ca.id_servicio,
        it_ca.id_sentido,
        it_ca.n_pc,
        it_ca.hh_control,
        t_max,
        it_ca.adelanto,
        it_ca.atraso,
        e.id_expedicion,
        e.id_vehiculo,
        e.hh_inicio,
        e.hh_fin,
    	e.kpi_cumplimiento_pc as kpi_pc,
    	(86400*date_part('day', e.hh_fin - e.hh_inicio) + 3600*date_part('hour', e.hh_fin - e.hh_inicio) +
    		60*date_part('minute', e.hh_fin - e.hh_inicio) + date_part('second', e.hh_fin - e.hh_inicio))/60/t_max as kpi_tv,
    	1 as kpi_itd,
    	case
    		when e.hh_inicio < it_ca.hh_control then
    			(86400*date_part('day', e.hh_inicio - it_ca.hh_control) + 3600*date_part('hour', e.hh_inicio - it_ca.hh_control) +
    			60*date_part('minute', e.hh_inicio - it_ca.hh_control) + date_part('second', e.hh_inicio - it_ca.hh_control))/(60*it_ca.adelanto)
    		else (86400*date_part('day', e.hh_inicio - it_ca.hh_control) + 3600*date_part('hour', e.hh_inicio - it_ca.hh_control) +
    			60*date_part('minute', e.hh_inicio - it_ca.hh_control) + date_part('second', e.hh_inicio - it_ca.hh_control))/(60*it_ca.atraso)
    	end as kpi_ith,
    	case when ve_ca.id_vehiculo is not null then 1::float else 0::float end as kpi_ve
    from it_ca
    inner join expedicion as e
        on  e.hh_inicio::date = it_ca.fe_fecha
        and e.id_pc = it_ca.id_pc
        and e.id_servicio = it_ca.id_servicio
        and e.id_sentido = it_ca.id_sentido
	left join ve_ca
    	on  ve_ca.fe_fecha = e.hh_inicio::date
    	and ve_ca.id_pc = e.id_pc
    	and ve_ca.id_servicio = e.id_servicio
    	and ve_ca.id_vehiculo = e.id_vehiculo
    where it_ca.operacion_programada = 1
    	and e.kpi_cumplimiento_pc = 1
    	and it_ca.adelanto is not null
    	and it_ca.atraso is not null
    	and e.hh_inicio between it_ca.hh_control - ( 3 * it_ca.adelanto * interval '1 minute')
    		and it_ca.hh_control + (3 * it_ca.atraso * interval '1 minute')
    order by it_ca.id_it_ca,
    	abs(86400*date_part('day', e.hh_inicio - it_ca.hh_control) + 3600*date_part('hour', e.hh_inicio - it_ca.hh_control) +
    	60*date_part('minute', e.hh_inicio - it_ca.hh_control) + date_part('second', e.hh_inicio - it_ca.hh_control))
),
/*
Get all indicators for the Expedition based on the Itinerary and Vehicle.

Similar to the above query with the difference that here it takes in count only the ones that were not included in the
conditions for Asignacion1a. The only condition is the fulfillment of the control point to be greater than 0.65.
*/
asignacion1b as(
    select
    	e.id_contrato,
        e.id_expedicion,
        e.id_pc,
        e.id_vehiculo,
        e.id_servicio,
        e.id_sentido,
        e.hh_inicio,
        e.hh_fin,
    	e.kpi_cumplimiento_pc as kpi_pc,
    	it_ca.operacion_programada,
    	case when it_ca.operacion_programada = 1 then 1 else 0 end as kpi_itd,
    	null::float as kpi_ith,
    	it_ca.t_max,
    	(86400*date_part('day', e.hh_fin - e.hh_inicio) + 3600*date_part('hour', e.hh_fin - e.hh_inicio) +
    		60*date_part('minute', e.hh_fin - e.hh_inicio) + date_part('second', e.hh_fin - e.hh_inicio))/60/it_ca.t_max as kpi_tv,
    	case when ve_ca.id_vehiculo is not null then 1::float else 0::float end as kpi_ve
    from expedicion as e
    left join asignacion1a as a1a
        on e.id_expedicion = a1a.id_expedicion
    	and e.id_contrato = a1a.id_contrato
    left join (select fe_fecha, id_pc, id_servicio, id_sentido, operacion_programada, max(t_max) t_max
			   from it_ca group by fe_fecha, id_pc, id_servicio, id_sentido, operacion_programada) as it_ca
    	on e.hh_inicio::date = it_ca.fe_fecha
        and e.id_pc = it_ca.id_pc
        and e.id_servicio = it_ca.id_servicio
        and e.id_sentido = it_ca.id_sentido
	left join ve_ca
    	on  ve_ca.fe_fecha = e.hh_inicio::date
    	and ve_ca.id_pc = e.id_pc
    	and ve_ca.id_servicio = e.id_servicio
    	and ve_ca.id_vehiculo = e.id_vehiculo
    where e.kpi_cumplimiento_pc >= 0.65
        and a1a.id_expedicion is null
),
/*
Uses Asignacion1a and Asignacion1b to creates a new table with both results.
*/
indicadores as(
        select
    		it_ca.id_contrato,
            it_ca.fe_fecha,
            it_ca.id_servicio,
            it_ca.id_sentido,
            it_ca.hh_control::time,
            it_ca.id_it_ca,
            a1a.id_expedicion,
    		a1a.id_vehiculo,
    		a1a.hh_inicio::time as hh_inicio,
    		a1a.hh_fin::time as hh_fin,
            a1a.kpi_pc,
            a1a.kpi_tv,
            a1a.kpi_ve,
            a1a.kpi_itd,
            a1a.kpi_ith
        from it_ca
        left join asignacion1a as a1a
            on a1a.id_it_ca = it_ca.id_it_ca
        where it_ca.operacion_programada = 1
            and it_ca.n_pc = 1
    union all
        select
    		id_contrato,
            hh_inicio::date as fe_fecha,
            id_servicio,
            id_sentido,
            null as hh_control,
            null as id_it_ca,
            id_expedicion,
    		id_vehiculo,
    		hh_inicio::time as hh_inicio,
    		hh_fin::time as hh_fin,
            kpi_pc,
            kpi_tv,
            kpi_ve,
            kpi_itd,
            kpi_ith
        from asignacion1b
    order by fe_fecha, id_servicio, id_sentido, hh_control, hh_inicio
)
/*
Insert all processed data into Indicators Dataset with their respective information.
*/
INSERT INTO {indicators_dataset} (
    dataset_name,
    dataset_table_name,
    id_contrato,
    id_servicio,
    id_sentido,
    fe_fecha,
    hh_control,
    id_vehiculo,
    hh_inicio,
    hh_fin,
    kpi_pc,
    kpi_tv,
    kpi_ve,
    kpi_itd,
    kpi_ith,
    kpi_cumplimiento_mes,
    rn_estado_mes,
    kpi_cumplimiento_dia,
    rn_estado_dia,
	kpi_cumplimiento_horario,
    rn_estado_horario
)
SELECT
	'{dataset_name}' as dataset_name,
	'{gps_dataset}' as dataset_table_name,
    id_contrato,
    id_servicio,
    id_sentido,
    fe_fecha,
    hh_control,
    id_vehiculo,
    hh_inicio,
    hh_fin,
    kpi_pc,
    kpi_tv,
    kpi_ve,
    kpi_itd,
    kpi_ith,
    case
    	when id_vehiculo is null then null
        when kpi_pc = 1 and kpi_tv <= 1 then 1 /*PROVISORIO HASTA QUERER INCORPORAR PPU EN PAGO. ORIGINAL: when kpi_pc = 1 and kpi_tv <= 1 then kpi_ve*/
        else 0
    end as kpi_cumplimiento_mes,
    case
    	when kpi_pc < 1 then 'No cumple Puntos de Control'
    	when kpi_tv > 1 then 'No cumple Tiempo de Viaje'
    	when kpi_ve = 0 then 'No cumple Vehículo'
    	when kpi_ith <= -2 or kpi_ith >= 2 then 'No cumple Itinerario'
        else null
    end as rn_estado_mes,
    case
    	when id_vehiculo is null then null
        when kpi_pc = 1 and kpi_tv <= 1 and kpi_itd = 1 then 1 /*PROVISORIO HASTA QUERER INCORPORAR PPU EN PAGO. ORIGINAL: when kpi_pc = 1 and kpi_tv <= 1 and kpi_itd = 1 then kpi_ve*/
        else 0
    end as kpi_cumplimiento_dia,
    case
    	when kpi_pc < 1 then 'No cumple Puntos de Control'
    	when kpi_tv > 1 then 'No cumple Tiempo de Viaje'
    	when kpi_ve = 0 then 'No cumple Vehículo'
    	when kpi_ith <= -2 or kpi_ith >= 2 then 'No cumple Itinerario'
        else null
    end as rn_estado_dia,
    case 
    	when id_vehiculo is null then null
        when kpi_pc = 1 and kpi_tv <= 1 then
    	case
            when kpi_ith is null then 0
            when kpi_ith >= -1 and kpi_ith <= 1 then 1 /*PROVISORIO HASTA QUERER INCORPORAR PPU EN PAGO. ORIGINAL: when kpi_ith >= -1 and kpi_ith <= 1 then least(1, kpi_ve)*/
            when kpi_ith <= -2 or kpi_ith >= 2 then 0
            else least(2-abs(kpi_ith), 1) /*PROVISORIO HASTA QUERER INCORPORAR PPU EN PAGO. ORIGINAL: else least(2-abs(kpi_ith), kpi_ve)*/
        end
    else 0 end as kpi_cumplimiento_horario,
    case
    	when kpi_pc < 1 then 'No cumple Puntos de Control'
    	when kpi_tv > 1 then 'No cumple Tiempo de Viaje'
    	when kpi_ve = 0 then 'No cumple Vehículo'
    	when kpi_ith <= -2 or kpi_ith >= 2 then 'No cumple Itinerario'
        else null
    end as rn_estado_horario
from indicadores
order by id_servicio, id_sentido, fe_fecha, hh_control, hh_inicio
