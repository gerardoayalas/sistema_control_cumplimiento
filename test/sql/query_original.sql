--CONTROL DE CONTRATOS 

--0.- DECLARACION DE VARIABLES
with
gps0 as (select * from dataset_214590),
contrato as ( select distinct Nombre_Contrato as id_contrato from gps0 ),
fe_ini as( select min(date_trunc('month', GPS_Fecha_Hora_Chile)::date) as fe_ini from gps0 ),
fe_fin as( select min(date_trunc('month', GPS_Fecha_Hora_Chile) + interval '1 month - 1 day')::date as fe_fin from gps0 ),

--1.- CONSTRUIR CALENDARIO DE PO EN BASE A LOS DATOS DE TRACKING QUE ESTOY INGRESANDO (KEYS: CONTRATO Y FECHA)
-- 1.1.- Obtengo calendario de Programas de Operación Base
-- NOTA: Acá y en query "gps" utilizo la data del gps. Dependiendo de cómo se defina la automatización
-- del proceso, se deberá modificar para dejarlo dinámico mediante variable o automatizado.
-- NOTA: Hay que agregar id_pc a los partition by y group by. Posibles redundancias en casos. REVISAR!!!
-- NOTA: Este código está diseñado en base a la Regla de Negocio de que una expedición no tiene un orden 
-- en la que los PC deben ejecutarse. En el caso de que se quiera exigir un orden en los PC y considerar 
-- reversibilidad (PC1>PC2>PC3>PC2>PC4) es necesaria una query a tabla PC del PO, con los movimientos 
-- permitidos desde un PC a otro (por ejemplo, del PC2 se puede ir al PC3 o al PC4), y en base a eso 
-- detectar y contabilizar sólo movimientos permitidos (con lead en query a tabla PO), contar el número de movimientos 
-- permitidos y contrastarlo con los movimientos programados (habría que utilizar unos distincts, 
-- para no contabilizar movimientos permitidos pero repetidos).

po as(
    select po.id_contrato,
        --po.fecha_inicio,--
        --po.fecha_fin,--
        fechas.fe_fecha,
        po.id_po,--
        po.id_it,
        po.id_pc,
        po.id_ve
    from dataset_214538 as po
    -- asigno/detecto PO para cada contrato y fecha en el rango definido (se duplican los PO para cada fecha)
    inner join (select dd.fe_fecha::date from generate_series( (select * from fe_ini) , (select * from fe_fin) , '1 day'::interval) dd(fe_fecha)) as fechas --VARIABLE
        on fechas.fe_fecha between po.fecha_inicio and po.fecha_fin
    where po.id_contrato = (select * from contrato) --VARIABLE
),
-- 1.2.- Obtengo calendario de Programas de Operación Excepcionales
poe as(
    select
       id_contrato,
       fecha as fe_fecha,
       id_po,--
       id_it,
       id_pc,
       id_ve
	from dataset_214529
	-- asigno/detecto PO para cada contrato y fecha en el rango definido
	where id_contrato = (select * from contrato) --VARIABLE
		and fecha between (select * from fe_ini)
    	and (select * from fe_fin) --VARIABLE
),
-- 1.3.- Obtengo calendario uniendo los 2 tipos de Programas de Operación. Excepcionales reemplazan a los Base
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
    	-- NOTA!!!! Se supone que al usar "union" eliminaría duplicados que se puedan generar en la segunda query, pero debo confirmar... se puede arreglar con un select distinct
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

-- 1.4.- Obtengo puntos de control (pc) para query "po_ca" y los transformo en puntos.
-- Comentario: En otra query, se deberán filtrar los puntos del tracking en base a los radios definidos en el PO
-- NOTA!! generar query que detecte pc iguales (misma lat y lon) y generar filtro en base a esa data, para tener
-- una query más eficiente.

pc_ca as(
    select distinct on (po_ca.id_pc, po_ca.fe_fecha, pc.id_servicio, pc.id_sentido, pc.n_pc)
        -- revisar si elimina duplicados (diferentes id_contrato/id_po con mismo id_pc)
        po_ca.id_contrato,
        po_ca.fe_fecha,
        --po_ca.id_po,
        po_ca.id_pc,
        pc.id_servicio,
        pc.id_sentido,
        pc.n_pc,
        pc.radio,
        --lat,--
        --lon,--
        ST_MakePoint(pc.lon::numeric, pc.lat::numeric) as point_pc
    from po_ca
    left join dataset_214530 as pc
        on pc.id_contrato = po_ca.id_contrato
        and pc.id_pc = po_ca.id_pc
    order by po_ca.id_pc, po_ca.fe_fecha, pc.id_servicio, pc.id_sentido, pc.n_pc
),

-- 2.- PREPARO INFORMACIÓN DE TRACKING PARA CRUZAR CON PUNTOS DE CONTROL
-- 2.1.- Consulto datos gps a analizar y creo puntos geográficos
gps as(
    select
        -- NOTA!!! Terminado este código, revisar acá para hacer row_number con "partition by"
        row_number () over(partition by PPU order by PPU, GPS_Fecha_Hora_Chile) as id_gps,
        Nombre_Contrato as id_contrato,
        PPU as id_vehiculo, --modificar PO
        GPS_Fecha_Hora_Chile as fechahora_local,
        GPS_Fecha_Hora_Chile::date as fe_fecha, --ELIMINAR Y HACER JOIN CON fechahora_local::date
        GPS_Fecha_Hora_Chile::time as hh_hora,--
        --direccion,--
        ST_MakePoint(GPS_Longitud::numeric, GPS_Latitud::numeric) as point_gps
    from gps0 -- VARIABLE
    where Nombre_Contrato = (select * from contrato) --VARIABLE (no es necesario este filtro, pero podría ayudar en automatización)
    order by PPU, GPS_Fecha_Hora_Chile
),
-- 3.- DETECTA EVENTOS DE ENTRADA Y SALIDAS DE GEOCERCA
-- 3.1.1.- Filtra pulsos GPS mediante un join a calendario de puntos de control (pc_ca)
        -- Dadas las restricciones con las que me encontré, el filtro del join sólo permite filtrar con radio
        -- constante e igual al mayor radio exigido. En segunda tabla se procederá a filtrar correctamente, pero
        -- el primer filtro elimina parte de la carga asociada a volúmen de datos.
evento_pc0 as (
    select
    --gps.id_contrato,--
    gps.fe_fecha,--
    pc_ca.id_pc,
    gps.id_vehiculo,
    pc_ca.id_servicio,
    pc_ca.id_sentido,
    pc_ca.n_pc,
    gps.id_gps,
    --gps.point_gps,--
    -- con este campo se pueden filtrar las geocercas con radio dinámico en la siguiente query
    ST_DWithin(gps.point_gps::geography, pc_ca.point_pc::geography, pc_ca.radio) as dentro_pc
    from gps
    --join preliminar, filtrando geocercas con radio máximo. Se debe hacer el filtro fino más adelante 
    join pc_ca
    on gps.fe_fecha = pc_ca.fe_fecha
    and ST_DWithin(gps.point_gps::geography, pc_ca.point_pc::geography, (select max(radio) from pc_ca))
    --order by gps.id_vehiculo, pc.id_servicio, pc.id_sentido, pc.n_pc, gps.id_gps
),
-- 3.1.2.- Filtra pulsos GPS fuera de pc en "where" mediante lógica creada en query "evento_pc0" y detecta
		-- eventos de entrada y salida de los pc en base a geocerca definida
evento_pc as (
    select
    --id_contrato,--
    fe_fecha,--
    id_pc,
    id_vehiculo,
    id_servicio,
    id_sentido,
    n_pc,
    id_gps,
    --point_gps,--
    -- 1: entrada a pc, -1, salida de pc, 11: un mismo punto es tanto entrada como salida de pc (sólo un pulso registrado dentro de pc)
    case
    	-- primer registro dentro de la partición. Puede ser entrada y salida o sólo entrada
    	when lag(id_gps,1) over(partition by id_pc, id_vehiculo, id_servicio, id_sentido, n_pc order by id_pc, id_vehiculo, id_servicio, id_sentido, n_pc, id_gps) is null then
    		case
    			when id_gps+1 < lead(id_gps,1) over(partition by id_pc, id_vehiculo, id_servicio, id_sentido, n_pc order by id_pc, id_vehiculo, id_servicio, id_sentido, n_pc, id_gps) then 11
    			else 1
    		end
    -- último registro dentro de la partición. Puede ser entrada y salida o sólo salida
    	when lead(id_gps,1) over(partition by id_pc, id_vehiculo, id_servicio, id_sentido, n_pc order by id_pc, id_vehiculo, id_servicio, id_sentido, n_pc, id_gps) is null then
    		case
    			when id_gps-1 > lag(id_gps,1) over(partition by id_pc, id_vehiculo, id_servicio, id_sentido, n_pc order by id_pc, id_vehiculo, id_servicio, id_sentido, n_pc, id_gps) then 11
    			else -1
    		end
        when id_gps+1 < lead(id_gps,1) over(partition by id_pc, id_vehiculo, id_servicio, id_sentido, n_pc order by id_pc, id_vehiculo, id_servicio, id_sentido, n_pc, id_gps)
    		and id_gps-1 > lag(id_gps,1) over(partition by id_pc, id_vehiculo, id_servicio, id_sentido, n_pc order by id_pc, id_vehiculo, id_servicio, id_sentido, n_pc, id_gps) then 11
        when id_gps+1 < lead(id_gps,1) over(partition by id_pc, id_vehiculo, id_servicio, id_sentido, n_pc order by id_pc, id_vehiculo, id_servicio, id_sentido, n_pc, id_gps) then -1
        when id_gps-1 > lag(id_gps,1) over(partition by id_pc, id_vehiculo, id_servicio, id_sentido, n_pc order by id_pc, id_vehiculo, id_servicio, id_sentido, n_pc, id_gps) then 1
        else 0
    	end as estado_evento
    from evento_pc0
    --filtra pulsos en base a tamaño de geocerca definida en el PO. Lógica calculada en la query anterior
    where dentro_pc
    order by id_pc, id_vehiculo, id_servicio, id_sentido, n_pc, id_gps--
),
-- 3.2.- A través de 2 reglas de negocio, define si un evento (de inicio o de control) es válido o no
-- NOTA: Esta lógica sólo aplica para datos dentro de un PC. No considera cuando hay 2 eventos
-- independientes que incumplen alguna regla de negocio adicional, como retornar o pasar por el mismo PC de nuevo
evento_valido as(
    select
        --id_contrato,--
        fe_fecha,--
        id_pc,
        id_vehiculo,
        id_servicio,
        id_sentido,
        n_pc,
        id_gps,
        --estado_evento,--
        -- detecta si un evento es un inico válido según reglas de negocio definidas
        case
        -- sólo salida en el el primer pc (n_pc=1) puede ser inicio de expedición. Si hay salidas
        -- consecutivas en pc se considera sólo la última
    		when n_pc != 1 or n_pc = lead(n_pc,1) over(partition by id_pc, id_vehiculo, id_servicio, id_sentido order by id_pc, id_vehiculo, id_servicio, id_gps) then false
    		else true
    	end as inicio_valido,
    -- sólo entrada a n_pc>1 puede ser punto de control. Si hay consecutivas entradas en pc se considera
    -- sólo la primera
    	case
    		when n_pc = 1 or n_pc = lag(n_pc,1) over(partition by id_pc, id_vehiculo, id_servicio, id_sentido order by id_pc, id_vehiculo, id_servicio, id_gps) then false
    		else true
    	end as control_valido
    from evento_pc
    -- filtra eventos de inicio o de control, en base a lógica "estado_evento" definida en query "evento_pc"
    where (n_pc = 1 and (estado_evento = -1 or estado_evento = 11))
       or (n_pc != 1 and (estado_evento = 1 or estado_evento = 11))
    order by id_pc, id_vehiculo, id_servicio, id_sentido, id_gps
),
--4.- CONSTRUIR EXPEDICIONES
-- Construye expediciones en base a eventos detectados y las reglas de negocio definidas
--4.1.- Identifica inicios de recorrido
evento_recorrido as(
    select
        --id_contrato,--
        fe_fecha,--
    	id_pc,
        id_vehiculo,
        id_servicio,
        id_sentido,
        n_pc,
        id_gps,
        case
            when n_pc = 1 then 1
            -- ojo!!! fecha podrá traerme problemas con servicios de más de un día
    		-- NOTA!! POR QUÉ FILTRO POR FECHA? HACER PRUEBAS ELIMINANDO
            --when fe_fecha != lag(fe_fecha,1) over(order by id_vehiculo, id_servicio, id_sentido, id_gps) then 1
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
    from evento_valido
    where inicio_valido
       or control_valido
    order by id_pc, id_vehiculo, id_servicio, id_sentido, id_gps
),
--EVALUAR: para elegir sólo una pasada por PC, basta con un 'group by' y un min id_gps
--4.2.- Crea id_expedición para subconjuntos de puntos de control.
expedicion_id as
(
    select
        ER.fe_fecha,
        ER.id_pc,
        ER.id_vehiculo,
        ER.id_servicio,
        ER.id_sentido,
        ER.n_pc, 
        ER.id_gps,
        ER.inicio_recorrido,--
        -- Ojo: id_expedicion es único por batch de información
         sum( ER.inicio_recorrido )
        over( order by ER.id_pc, ER.id_vehiculo, ER.id_servicio, ER.id_sentido, ER.id_gps rows between unbounded preceding and current row
	    ) as id_expedicion,
        basura,
	nMaxPCCA.nMaximoPC
    from evento_recorrido ER
--Obtener máximo punto de control definido por diseño.
	      Left join (select id_pc, id_servicio, id_sentido, max(n_pc) as nMaximoPC
                           from pc_ca
                          group by id_pc, id_servicio, id_sentido
                          order by id_pc, id_servicio, id_sentido) as nMaxPCCA
	             on nMaxPCCA.id_pc       = ER.id_pc
                    and nMaxPCCA.id_servicio = ER.id_servicio   
                    and nMaxPCCA.id_sentido  = ER.id_sentido
    order by ER.id_pc, ER.id_vehiculo, ER.id_servicio, ER.id_sentido, ER.id_gps
),

--4.3.- Contabiliza pasadas por PC, conservando la primera para cada 'n_pc' y obtiene 'max_pc' para futura Regla de Negocio.
(
	-- Regla de Negocio: el distinct elimina eventos de punto de control duplicados. Dada la lógica que utiliza,
	--                   conserva el primer registro.
    select distinct on (eid.id_pc, eid.id_vehiculo, eid.id_servicio, eid.id_sentido, eid.id_expedicion, eid.n_pc)    
        eid.fe_fecha,
        eid.id_pc,
        eid.id_vehiculo,
        eid.id_servicio,
        eid.id_sentido,
        eid.id_expedicion,
        eid.n_pc,
        eid.nMaximoPC,
        eid.id_gps,
        gps.fechahora_local,
        eid.basura
    from expedicion_id as eid
    left join gps
    	on eid.id_vehiculo = gps.id_vehiculo
       and eid.id_gps      = gps.id_gps
            left join
	              ( select distinct on (eid.id_expedicion) eid.id_expedicion, eid.id_gps, pcca.nmax 
                          from expedicion_id eid                       
                          left join (  select id_pc, id_servicio, id_sentido, max(n_pc) as nmax
                                       from pc_ca
                                      group by id_pc, id_servicio, id_sentido
                                      order by id_pc, id_servicio, id_sentido ) AS pcca
                                 on pcca.id_pc       = eid.id_pc
                                and pcca.id_servicio = eid.id_servicio   
                                and pcca.id_sentido  = eid.id_sentido
                              where eid.n_pc         = pcca.nmax
                      ) AS expcca 
                   on eid.id_expedicion = expcca.id_expedicion
       where (eid.id_gps <= expcca.id_gps) OR (expcca.id_gps is null)           
   order by eid.id_pc, eid.id_vehiculo, eid.id_servicio, eid.id_sentido, eid.id_expedicion, eid.n_pc, eid.id_gps
),

--4.4.- Detecta si la expedición fue válida, en base a Reglas de Negocio anteriores.
expedicion0 as
(
    select
            epc.id_expedicion,
            epc.id_pc,
            epc.id_vehiculo,
            epc.id_servicio,
            epc.id_sentido,
            min(fechahora_local) as hh_inicio,
            max(fechahora_local) as hh_fin,
            --Regla de Negocio: calculada como %, por lo que se puede considerar de diferentes formas
            count(id_vehiculo)::decimal/max(nMaximoPC) as kpi_pc,
        case
            when sum(basura) > 0 then 1 when count(id_vehiculo)::decimal/max(nMaximoPC) < 0.65 then 1
            --Regla de Negocio: elimino expediciones que superen 1.5x el máximo tiempo definido para esa expedición.
    		--					Ver join para entender cómo se calcula el máximo.
            when date_part('hour', max(fechahora_local)   - min(fechahora_local)) * 3600 +
    		 date_part('minute', max(fechahora_local) - min(fechahora_local)) * 60 +
    		 date_part('second', max(fechahora_local) - min(fechahora_local)) >
    		 1.5 * max(t_max.t_max) * 60 then 1
            else 0
        end as basura,
        1 as check_traslape_salida -- cómo validar que un vehículo no esté operando al mismo tiempo en 2 servicios?
    from expedicion_pc as epc
    -- Regla de negocio: obtengo el máximo tiempo de viaje permitido (t_max) para un servicio-sentido,
    --					 en base a todos los itinerarios de ese is_pc
    left join (
                select
                       id_it.id_servicio,
                       id_it.id_sentido,
                       it_pc.id_pc,
                       max(id_it.t_max) as t_max
                  from dataset_214531 as id_it
                   inner join (select distinct id_it, id_pc from po_ca) as it_pc
        	           on it_pc.id_it = it_pc.id_it 
	                where id_contrato = (select * from contrato)
                        group by id_it.id_servicio, id_sentido, it_pc.id_pc
                        order by id_it.id_servicio, id_it.id_sentido, it_pc.id_pc
                              ) as t_max
    	   on t_max.id_servicio = epc.id_servicio
    	  and t_max.id_sentido = epc.id_sentido
    	  and t_max.id_pc = epc.id_pc
    --group by es por id_po. Revisar qué pasa cuando un viaje cambia de día y de PO (caso de borde).
    group by epc.id_pc, id_vehiculo, epc.id_servicio, epc.id_sentido, id_expedicion
    order by epc.id_pc, id_vehiculo, epc.id_servicio, epc.id_sentido, id_expedicion
),
			   
expedicion1 as
(
 select
        e0.id_expedicion,
        e0.id_pc,
        e0.id_vehiculo,
        e0.id_servicio,
        e0.id_sentido,
        e0.hh_inicio,
        e0.hh_fin,
        --ini.ini_prioritario,
        --fin.fin_prioritario,
        e0.kpi_pc,
    	case
            when e0.kpi_pc != 1 then case
                when e0.hh_inicio < lag(e0.hh_fin, 1) over (partition by e0.id_vehiculo order by e0.id_vehiculo, e0.hh_inicio, e0.kpi_pc desc, e0.hh_fin desc)
                    --Regla de Negocio: Sólo eliminará (valor 1) cuando el anterior tiene kpi_pc=1. Eliminar esta regla y elimina todo lo traslapado. 
    				and lag(e0.kpi_pc, 1) over (partition by e0.id_vehiculo order by e0.id_vehiculo, e0.hh_inicio, e0.kpi_pc desc, e0.hh_fin desc) = 1
                then 1
                when e0.hh_fin > lead(e0.hh_inicio, 1) over (partition by e0.id_vehiculo order by e0.id_vehiculo, e0.hh_inicio, e0.kpi_pc desc, e0.hh_fin desc)
    				--Regla de Negocio: Sólo eliminará (valor 1) cuando el posterior tiene kpi_pc=1. Elminar esta regla y elimina todo lo traslapado. 
                    and lead(e0.kpi_pc, 1) over (partition by e0.id_vehiculo order by e0.id_vehiculo, e0.hh_inicio, e0.kpi_pc desc, e0.hh_fin desc) =1
                then 1
    			else 0
    		end
            else 0
        end as traslape --¿cambiar a 'basura'?
 from expedicion0 as e0
    -- Regla de Negocio: El inner join elimina cualquier duplicado, dejando sólo una. En el caso de que hayan dos con 100% pc sólo quedará la más larga.
    inner join (
        -- ordeno por ed_vehiculo y hh_ini y kpi_pc, para posteriormente dejar la expedición con mayor cumplimiento
        -- con el disinct on. En caso de empate queda la con mayor tiempo de viaje.
                  select distinct on (id_vehiculo, hh_inicio)
                                      id_expedicion,
                                      id_vehiculo,
                                      hh_inicio,
                                      1 as ini_prioritario
                    from expedicion0
                   where basura = 0
                   order by id_vehiculo, hh_inicio, kpi_pc desc, hh_fin desc
               ) as ini
    	    on ini.id_expedicion = e0.id_expedicion
    -- Regla de Negocio: El inner join elimina cualquier duplicado, dejando sólo una. En el caso de que hayan dos con 100% pc sólo quedará la más larga.
          inner join (
        -- ordeno por ed_vehiculo y hh_fin y kpi_pc, para posteriormente dejar la expedición con mayor cumplimiento
        -- con el disinct on. En caso de empate queda la con mayor tiempo de viaje.
                        select distinct on (id_vehiculo, hh_fin)
                                            id_expedicion,
                                            id_vehiculo,
                                            hh_fin,
                                            1 as fin_prioritario
                          from expedicion0
                         where basura = 0
                         order by id_vehiculo, hh_fin, kpi_pc desc, hh_inicio
                     ) as fin on fin.id_expedicion = e0.id_expedicion
    where e0.basura = 0
    order by e0.id_vehiculo, e0.hh_inicio, e0.kpi_pc desc, e0.hh_fin desc
), --ÚLTIMA QUERY DE PRIMER PROCESO. PARA OBTENER EL OUTPUT, DEBE FILTRARSE POR TRASLAPE=0

--5.- CUMPLIMIENTO ITINERARIO Y PPU
--5.1. 
-- NOTA!! ESTOY CREANDO PO PARA DÍAS QUE NO OPERA, REVISAR SI SE PUEDE ELIMINAR O ES NECESARIO.
it_ca as(
    select
    	row_number() over(order by po_ca.id_contrato,po_ca.fe_fecha,po_ca.id_po,po_ca.id_it,it.id_servicio,it.id_sentido, it.n_pc, it.hh_control) as id_it_ca,
        --po_ca.id_contrato,
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
        case --NOTA!!!! transformar los días (0 o 1) a binarias antes de cargar la info.
            when date_part('dow', po_ca.fe_fecha) = 0 then it.dom
            when date_part('dow', po_ca.fe_fecha) = 1 then it.lun
            when date_part('dow', po_ca.fe_fecha) = 2 then it.mar
            when date_part('dow', po_ca.fe_fecha) = 3 then it.mie
            when date_part('dow', po_ca.fe_fecha) = 4 then it.jue
            when date_part('dow', po_ca.fe_fecha) = 5 then it.vie
            when date_part('dow', po_ca.fe_fecha) = 6 then it.sab
        end as operacion_programada
    from po_ca
    left join dataset_214531 as it
        on po_ca.id_contrato = it.id_contrato
        and po_ca.id_it = it.id_it
    where it.n_pc = 1 -- OJO!!! SÓLO PARA ITINERARIO EN PC=1
    order by po_ca.fe_fecha, po_ca.id_po, po_ca.id_it, it.id_servicio, it.id_sentido, it.hh_control
),
--SIMPLIFICAR!!! DEBIESE HABER UN JOIN Y LISTO
ve_ca as(
	select
    	row_number() over(order by po_ca.fe_fecha, po_ca.id_po, po_ca.id_ve, ve.id_servicio, ve.id_vehiculo) as id_ve_ca,
        --po_ca.id_contrato,
        po_ca.fe_fecha,
        po_ca.id_po,--
        po_ca.id_ve,
        po_ca.id_it,--
        po_ca.id_pc,--
    	ve.id_servicio,
    	ve.id_vehiculo
    from po_ca
    left join dataset_214532 as ve
        on po_ca.id_contrato = ve.id_contrato
        and po_ca.id_ve = ve.id_ve
    order by po_ca.fe_fecha, po_ca.id_po, po_ca.id_ve, ve.id_servicio
),
asignacion1a as(
    -- NOTA: esto debería abordarse en un while/loop, que itere mientras haya más de un viaje asociado a un viaje programado
    -- OJO!!! ESTO SÓLO ESTÁ ELIMINANDO CUANDO UN VIAJE PROGRAMADO QUEDA CON MÁS DE UN VIAJE EJECUTADO VÁLIDO,
	       -- PERO NO CUANDO UN VIAJE EJECUTADO QUEDA VÁLIDO EN MÁS DE UN VIAJE PROGRAMADO. VER CÓMO DEJAR SÓLO 1
    select distinct on(it_ca.id_it_ca)
    	-- NOTA: eliminar la mayor cantidad de columnas una vez cerrado y validado el código,
    		  -- con id_it_ca se puede trabajar todo (creo)
        it_ca.id_it_ca,
        it_ca.fe_fecha,--del
        --it_ca.id_po,--del
        it_ca.id_it,--del
        it_ca.id_servicio,--del
        it_ca.id_sentido,--del
        it_ca.n_pc, --OJO!!! ARMAR MÁS ADELANTE LÓGICA PARA EVALUAR MÚLTIPLES PUNTOS DE CONTROL
        it_ca.hh_control,--del
        t_max,--del
        it_ca.adelanto,--del
        it_ca.atraso,--del
        e.id_expedicion,
        e.id_vehiculo,--del
        e.hh_inicio,--del
        e.hh_fin,--del
    	e.kpi_pc,
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
    -- considera sólo cruce de información
    inner join (select * from expedicion1 where traslape = 0) as e
        on  e.hh_inicio::date = it_ca.fe_fecha
        and e.id_pc = it_ca.id_pc
        and e.id_servicio = it_ca.id_servicio
        and e.id_sentido = it_ca.id_sentido
	left join ve_ca
    	on  ve_ca.fe_fecha = e.hh_inicio::date
    	and ve_ca.id_pc = e.id_pc
    	and ve_ca.id_servicio = e.id_servicio
    	and ve_ca.id_vehiculo = e.id_vehiculo
    where it_ca.operacion_programada = 1 -- no es necesario hacer match para días que no hay operación programada
    	and e.kpi_pc = 1 -- Regla de Negocio: Sólo 100% cumplimiento
    	and it_ca.adelanto is not null
    	and it_ca.atraso is not null
    	-- Regla de Negocio: sólo considera expediciones que estén dentro de un intervalo de tiempo adecuado (3 veces adelanto/atraso).
        -- También apoya en no considerar ciertos viajes para match en loop
    	-- NOTA: Expediciones que no tengan definidas adelanto/atraso quedan en otro proceso que solo cuenta.
    	and e.hh_inicio between it_ca.hh_control - ( 3 * it_ca.adelanto * interval '1 minute')
    		and it_ca.hh_control + (3 * it_ca.atraso * interval '1 minute')
    order by it_ca.id_it_ca,
    	abs(86400*date_part('day', e.hh_inicio - it_ca.hh_control) + 3600*date_part('hour', e.hh_inicio - it_ca.hh_control) +
    	60*date_part('minute', e.hh_inicio - it_ca.hh_control) + date_part('second', e.hh_inicio - it_ca.hh_control))
),
asignacion1b as(
    select
        e.id_expedicion,
        e.id_pc,
        e.id_vehiculo,
        e.id_servicio,
        e.id_sentido,
        e.hh_inicio,
        e.hh_fin,
    	e.kpi_pc,
    	it_ca.operacion_programada,
    	case when it_ca.operacion_programada = 1 then 1 else 0 end as kpi_itd,
    	null::float as kpi_ith,
    	it_ca.t_max,
    	(86400*date_part('day', e.hh_fin - e.hh_inicio) + 3600*date_part('hour', e.hh_fin - e.hh_inicio) +
    		60*date_part('minute', e.hh_fin - e.hh_inicio) + date_part('second', e.hh_fin - e.hh_inicio))/60/it_ca.t_max as kpi_tv,
    	case when ve_ca.id_vehiculo is not null then 1::float else 0::float end as kpi_ve
    from expedicion1 as e
    left join asignacion1a as a1a
        on e.id_expedicion = a1a.id_expedicion
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
    -- Aunque la RN definida en los puntos de control es sólo considerar 100% de cumplimiento,
    -- se traen sobre 65%, con el fin de generar atributos de incumplimiento (no cumple trazado).
    where traslape = 0
    	and e.kpi_pc >= 0.65
        and a1a.id_expedicion is null
),
indicadores as(
        select
            it_ca.fe_fecha,--del
            it_ca.id_servicio,--del
            it_ca.id_sentido,--del
            it_ca.hh_control::time,--del
            it_ca.id_it_ca,
            a1a.id_expedicion,
    		a1a.id_vehiculo,
    		a1a.hh_inicio::time as hh_inicio,
    		a1a.hh_fin::time as hh_fin,
            a1a.kpi_pc, 	--OK
            a1a.kpi_tv, 	--OK
            a1a.kpi_ve,	--OK
            a1a.kpi_itd,	--OK
            a1a.kpi_ith	--OK
        from it_ca
        left join asignacion1a as a1a
            on a1a.id_it_ca = it_ca.id_it_ca
        where it_ca.operacion_programada = 1
            and it_ca.n_pc = 1 --NOTA: revisar cuando se incorpore lógica de horarios intermedios.
    union all
        select
            hh_inicio::date as fe_fecha,--
            id_servicio,--
            id_sentido,--
            null as hh_control,--
            null as id_it_ca,
            id_expedicion,
    		id_vehiculo,
    		hh_inicio::time as hh_inicio,
    		hh_fin::time as hh_fin,
            kpi_pc,	--OK
            kpi_tv,	--OK
            kpi_ve,	--OK
            kpi_itd,	--OK
            kpi_ith	--OK
        from asignacion1b
    order by fe_fecha, id_servicio, id_sentido, hh_control, hh_inicio
),
asd as(
-- nota: Evaluar agregar id_po, id_it, id_pc y id_ve
-- OJO!! ESTA LÓGICA NO CONSIDERA EL TRUNCAR POR DÍA O ITINERARIO, POR LO QUE SE PUEDEN COMPENSAR HORARIOS, CREAR SIGUIENTE QUERY QUE AGRUPE
select
    (select * from contrato) as id_contrato,
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
    -- Regla de Negocio: cuando cumple puntos de control y tiempo de viaje, el puntaje máximo a obtener es el de kpi_ve.
    case
    	when id_vehiculo is null then null
        when kpi_pc = 1 and kpi_tv <= 1 then kpi_ve
        else 0
    end as kpi_cumplimiento_mes,
    -- Regla de Negocio: cuando cumple puntos de control, tiempo de viaje y opera en día de oepración, el puntaje máximo a obtener es el de kpi_ve.
    case
    	when id_vehiculo is null then null
        when kpi_pc = 1 and kpi_tv <= 1 and kpi_itd = 1 then kpi_ve
        else 0
    end as kpi_cumplimiento_dia,
    case 
    	when id_vehiculo is null then null
        when kpi_pc = 1 and kpi_tv <= 1 then
    	case
            when kpi_ith is null then 0
            when kpi_ith >= -1 and kpi_ith <= 1 then least(1, kpi_ve)
            when kpi_ith <= -2 or kpi_ith >= 2 then 0
            else least(2-abs(kpi_ith), kpi_ve)
        end
    else 0 end as kpi_cumplimiento_horario,
    case
    	when kpi_pc < 1 then 'No cumple Puntos de Control'
    	when kpi_tv > 1 then 'No cumple Tiempo de Viaje'
    	when kpi_ve = 0 then 'No cumple Vehículo'
    	when kpi_ith <= -2 or kpi_ith >= 2 then 'No cumple Itinerario'
        --when ??? then 'Sobreoferta'
        else null
    end as rn_estado,
    count(hh_control) over (
        partition by id_servicio, id_sentido
        order by id_servicio, id_sentido, fe_fecha, hh_control, hh_inicio
        rows between unbounded preceding and current row
    ) as programacion_acumulada,
    count(hh_inicio) over (
        partition by id_servicio, id_sentido
        order by id_servicio, id_sentido, fe_fecha, hh_control, hh_inicio
   		rows between unbounded preceding and current row
    ) as operacion_acumulada
from indicadores
order by id_servicio, fe_fecha, id_sentido, hh_control, hh_inicio
)

select * from asd
