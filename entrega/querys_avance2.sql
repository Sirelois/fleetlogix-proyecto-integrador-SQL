-- Query 1: Contar vehículos por tipo
EXPLAIN ANALYZE SELECT vehicle_type, COUNT(*) AS cantidad 
FROM vehicles 
GROUP BY vehicle_type
ORDER BY cantidad DESC;

-- Query 2: Conductores con licencia próxima a vencer
EXPLAIN ANALYZE SELECT driver_id, first_name, last_name, license_expiry 
FROM drivers 
WHERE license_expiry BETWEEN CURRENT_DATE AND CURRENT_DATE + 30
ORDER BY license_expiry ASC;

-- Query 3: Total de viajes por estado
EXPLAIN ANALYZE SELECT status, COUNT(*) AS total 
FROM trips 
GROUP BY status;

-- Query 4: Total de entregas por ciudad destino en los últimos 2 meses
EXPLAIN ANALYZE SELECT r.destination_city, COUNT(*) AS total_entregas
FROM deliveries d
JOIN trips t ON d.trip_id = t.trip_id
JOIN routes r ON t.route_id = r.route_id
WHERE d.delivered_datetime >= CURRENT_DATE - INTERVAL '2 months'
GROUP BY r.destination_city
ORDER BY total_entregas DESC;

-- Query 5: Conductores activos con cantidad de viajes completados
EXPLAIN ANALYZE SELECT d.driver_id, d.first_name, d.last_name, COUNT(t.trip_id) AS viajes_completados
FROM drivers d
JOIN trips t ON d.driver_id = t.driver_id
WHERE d.status = 'active' AND t.status = 'completed'
GROUP BY d.driver_id, d.first_name, d.last_name
ORDER BY viajes_completados DESC;

-- Query 6: Promedio de entregas por conductor en los últimos 6 meses
EXPLAIN ANALYZE SELECT d.driver_id, d.first_name, d.last_name, 
       ROUND(AVG(entregas_por_viaje), 2) AS promedio_entregas
FROM drivers d
JOIN trips t ON d.driver_id = t.driver_id
JOIN (
    SELECT trip_id, COUNT(*) AS entregas_por_viaje
    FROM deliveries
    GROUP BY trip_id
) sub ON t.trip_id = sub.trip_id
WHERE t.departure_datetime >= CURRENT_DATE - INTERVAL '6 months'
GROUP BY d.driver_id, d.first_name, d.last_name
ORDER BY promedio_entregas DESC;

-- Query 7: Rutas con mayor consumo de combustible por kilómetro
EXPLAIN ANALYZE SELECT r.route_code, r.origin_city, r.destination_city,
       ROUND(AVG(t.fuel_consumed_liters / r.distance_km), 4) AS consumo_por_km
FROM trips t
JOIN routes r ON t.route_id = r.route_id
WHERE r.distance_km > 0
GROUP BY r.route_id, r.route_code, r.origin_city, r.destination_city
ORDER BY consumo_por_km DESC
LIMIT 10;

-- Query 8: Análisis de entregas retrasadas por día de la semana
EXPLAIN ANALYZE SELECT 
    TO_CHAR(d.scheduled_datetime, 'Day') AS dia_semana,
    EXTRACT(DOW FROM d.scheduled_datetime) AS num_dia,
    COUNT(*) AS total_entregas,
    SUM(CASE WHEN d.delivered_datetime > d.scheduled_datetime + INTERVAL '30 minutes' 
        THEN 1 ELSE 0 END) AS entregas_retrasadas,
    ROUND(100.0 * SUM(CASE WHEN d.delivered_datetime > d.scheduled_datetime + INTERVAL '30 minutes' 
        THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_retraso
FROM deliveries d
WHERE d.delivery_status = 'delivered'
GROUP BY dia_semana, num_dia
ORDER BY num_dia;

-- Query 9: Costo de mantenimiento por kilómetro recorrido
EXPLAIN ANALYZE SELECT v.vehicle_type,
       ROUND(SUM(m.cost), 2) AS costo_total_mantenimiento,
       ROUND(SUM(t.fuel_consumed_liters * r.distance_km), 2) AS km_totales,
       ROUND(SUM(m.cost) / NULLIF(SUM(r.distance_km), 0), 2) AS costo_por_km
FROM vehicles v
JOIN maintenance m ON v.vehicle_id = m.vehicle_id
JOIN trips t ON v.vehicle_id = t.vehicle_id
JOIN routes r ON t.route_id = r.route_id
GROUP BY v.vehicle_type
ORDER BY costo_por_km DESC;

-- Query 10: Ranking de conductores por eficiencia usando Window Functions
EXPLAIN ANALYZE WITH conductor_stats AS (
    SELECT d.driver_id, d.first_name, d.last_name,
           COUNT(t.trip_id) AS total_viajes,
           ROUND(AVG(t.fuel_consumed_liters / NULLIF(r.distance_km, 0)), 4) AS consumo_promedio,
           COUNT(del.delivery_id) AS total_entregas
    FROM drivers d
    JOIN trips t ON d.driver_id = t.driver_id
    JOIN routes r ON t.route_id = r.route_id
    JOIN deliveries del ON t.trip_id = del.trip_id
    WHERE t.status = 'completed'
    GROUP BY d.driver_id, d.first_name, d.last_name
)
SELECT *,
       RANK() OVER (ORDER BY total_entregas DESC, consumo_promedio ASC) AS ranking
FROM conductor_stats
ORDER BY ranking
LIMIT 20;

-- Query 11: Análisis de tendencia de viajes con LAG y LEAD
EXPLAIN ANALYZE WITH viajes_mensuales AS (
    SELECT 
        DATE_TRUNC('month', departure_datetime) AS mes,
        COUNT(*) AS total_viajes
    FROM trips
    GROUP BY mes
)
SELECT 
    mes,
    total_viajes,
    LAG(total_viajes) OVER (ORDER BY mes) AS mes_anterior,
    LEAD(total_viajes) OVER (ORDER BY mes) AS mes_siguiente,
    total_viajes - LAG(total_viajes) OVER (ORDER BY mes) AS variacion
FROM viajes_mensuales
ORDER BY mes;

-- Query 12: Pivot de entregas por hora y día de la semana
EXPLAIN ANALYZE SELECT 
    EXTRACT(HOUR FROM scheduled_datetime) AS hora,
    SUM(CASE WHEN EXTRACT(DOW FROM scheduled_datetime) = 1 THEN 1 ELSE 0 END) AS lunes,
    SUM(CASE WHEN EXTRACT(DOW FROM scheduled_datetime) = 2 THEN 1 ELSE 0 END) AS martes,
    SUM(CASE WHEN EXTRACT(DOW FROM scheduled_datetime) = 3 THEN 1 ELSE 0 END) AS miercoles,
    SUM(CASE WHEN EXTRACT(DOW FROM scheduled_datetime) = 4 THEN 1 ELSE 0 END) AS jueves,
    SUM(CASE WHEN EXTRACT(DOW FROM scheduled_datetime) = 5 THEN 1 ELSE 0 END) AS viernes,
    SUM(CASE WHEN EXTRACT(DOW FROM scheduled_datetime) = 6 THEN 1 ELSE 0 END) AS sabado,
    SUM(CASE WHEN EXTRACT(DOW FROM scheduled_datetime) = 0 THEN 1 ELSE 0 END) AS domingo
FROM deliveries
GROUP BY hora
ORDER BY hora;


-- =====================================================
-- FLEETLOGIX - ÍNDICES DE OPTIMIZACIÓN
-- Basados en las 12 queries analizadas
-- Objetivo: Mejorar performance en 20%+
-- =====================================================

-- Análisis de performance ANTES de crear índices
-- Ejecutar cada query con EXPLAIN ANALYZE y guardar tiempos

-- =====================================================
-- ÍNDICE 1: Optimización para JOINs frecuentes en trips
-- =====================================================
-- Justificación: Las queries 4-12 hacen JOIN intensivo entre trips y otras tablas
-- Queries beneficiadas: 4, 5, 6, 7, 9, 10, 11
CREATE INDEX idx_trips_composite_joins ON trips(vehicle_id, driver_id, route_id, departure_datetime)
WHERE status = 'completed';

-- =====================================================
-- ÍNDICE 2: Optimización para análisis temporal de deliveries
-- =====================================================
-- Justificación: Queries 8, 12 filtran y agrupan por scheduled_datetime
-- Queries beneficiadas: 4, 8, 12
CREATE INDEX idx_deliveries_scheduled_datetime ON deliveries(scheduled_datetime, delivery_status)
WHERE delivery_status = 'delivered';

-- =====================================================
-- ÍNDICE 3: Optimización para mantenimiento por vehículo
-- =====================================================
-- Justificación: Query 9 necesita acceso rápido a mantenimientos por vehículo
-- Queries beneficiadas: 9
CREATE INDEX idx_maintenance_vehicle_cost ON maintenance(vehicle_id, cost);

-- =====================================================
-- ÍNDICE 4: Optimización para análisis de conductores
-- =====================================================
-- Justificación: Queries 5, 6, 10 filtran por conductores activos
-- Queries beneficiadas: 2, 5, 6, 10
CREATE INDEX idx_drivers_status_license ON drivers(status, license_expiry)
WHERE status = 'active';

-- =====================================================
-- ÍNDICE 5: Optimización para métricas de rutas
-- =====================================================
-- Justificación: Query 7 calcula consumo por ruta
-- Queries beneficiadas: 4, 7, 9, 10
CREATE INDEX idx_routes_metrics ON routes(route_id, distance_km, destination_city);

-- =====================================================
-- COMANDOS PARA VERIFICAR ÍNDICES CREADOS
-- =====================================================
SELECT 
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE schemaname = 'public'
    AND indexname LIKE 'idx_%'
ORDER BY tablename, indexname;

-- =====================================================
-- MANTENIMIENTO DE ÍNDICES
-- =====================================================
ANALYZE vehicles;
ANALYZE drivers;
ANALYZE routes;
ANALYZE trips;
ANALYZE deliveries;
ANALYZE maintenance;