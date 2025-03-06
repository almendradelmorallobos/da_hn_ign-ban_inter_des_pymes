-- -- Conteo de NUME_NIDES

-- SELECT 
-- EMPRESA
-- , RAZON
-- , COUNT(NUME_NIDE) as IDS
-- , COUNT(DISTINCT(NUME_NIDE)) as IDS_dist
-- FROM `dfa-dna-ws0017-la-prd-2423.DS_INFO_LABORAL_HN.HN_TTEL_DIR_EXTERNA`
-- GROUP BY ALL
-- ORDER BY 3 DESC

-----------------------------------------------------------------------------------------


WITH PADRON as (

SELECT 
cluster_id
-- , RAZON
, a.NUME_NIDE
, b.tipopersona
FROM `dfa-dna-ws0017-la-prd-2423.dfa_dna_ws0017_la_prd_sandbox.HN_TTEL_DIR_EXTERNA_CLUSTERIZADO` a
  LEFT JOIN (SELECT 
              nume_nide
              , tipopersona
             FROM `df-dna-dmt-la-prd-98d2.hn_cb_la_prd.general_hash_la_prd` 
             WHERE archive = '2024-10-15'  
             GROUP BY ALL) b
    ON a.NUME_NIDE = b.nume_nide
-- WHERE a.NUME_NIDE = '2001823366'
-- WHERE a.NUME_NIDE = '0000003080' and EMPRESA = 'SECRETARIA DE EDUCACION PUBLICA'
-- WHERE EMPRESA = 'SECRETARIA DE EDUCACION PUBLICA'-
WHERE b.tipopersona = 'EMP'
GROUP BY ALL
ORDER BY 3 DESC
)

-- SELECT * FROM PADRON

, General as (
SELECT
'Bandeuda' as fuente
, nume_nide
, b.periodoinformacion
, b.numerooperacion as cuenta
, count(1) as registros
FROM `df-dna-dmt-la-prd-98d2.hn_cb_la_prd.general_hash_la_prd` a, a.bandeudadet b
WHERE archive = '2024-10-15'
GROUP BY ALL

UNION ALL

SELECT
'Tardeuda' as fuente
, nume_nide
, b.periodoinformacion
, b.numeroreferenciatarjeta as cuenta
, count(1) as registros
FROM `df-dna-dmt-la-prd-98d2.hn_cb_la_prd.general_hash_la_prd` a, a.tardeudadet b
WHERE archive = '2024-10-15'
GROUP BY ALL

UNION ALL

SELECT
'Deucomer' as fuente
, nume_nide
, b.periodoinformaciondeu as periodoinformacion
, b.numerocuenta as cuenta
, count(1) as registros
FROM `df-dna-dmt-la-prd-98d2.hn_cb_la_prd.general_hash_la_prd` a, a.deudcomerdet b
WHERE archive = '2024-10-15'
GROUP BY ALL
)


, T1 as (
SELECT
P.*
, COUNT(DISTINCT(CASE WHEN G.fuente = 'Bandeuda' THEN cuenta END)) as cuentas_bandeuda
, SUM(CASE WHEN G.fuente = 'Bandeuda' THEN registros END) as reg_bandeuda
, MIN(CASE WHEN G.fuente = 'Bandeuda' THEN periodoinformacion END) as min_periinfo_bandeuda
, MAX(CASE WHEN G.fuente = 'Bandeuda' THEN periodoinformacion END) as max_periinfo_bandeuda

, COUNT(DISTINCT(CASE WHEN G.fuente = 'Tardeuda' THEN cuenta END)) as cuentas_tardeuda
, SUM(CASE WHEN G.fuente = 'Tardeuda' THEN registros END) as reg_tardeuda
, MIN(CASE WHEN G.fuente = 'Tardeuda' THEN periodoinformacion END) as min_periinfo_tardeuda
, MAX(CASE WHEN G.fuente = 'Tardeuda' THEN periodoinformacion END) as max_periinfo_tardeuda

, COUNT(DISTINCT(CASE WHEN G.fuente = 'Deucomer' THEN cuenta END)) as cuentas_deucomer
, SUM(CASE WHEN G.fuente = 'Deucomer' THEN registros END) as reg_deucomer
, MIN(CASE WHEN G.fuente = 'Deucomer' THEN periodoinformacion END) as min_periinfo_deucomer
, MAX(CASE WHEN G.fuente = 'Deucomer' THEN periodoinformacion END) as max_periinfo_deucomer


FROM PADRON P
  LEFT JOIN GENERAL G
    ON P.NUME_NIDE = G.NUME_NIDE
GROUP BY ALL
)


-- SELECT *
-- FROM T1
-- ORDER BY NUMe_NIDE

-- SELECT 
-- CASE WHEN reg_bandeuda > 0 then '>0' 
--      WHEN reg_bandeuda is null then 'null'
--      ELSE 'Otro' END,  
-- COUNT(DISTINCT(NUME_NIDE)) as IDS,
-- count(1) as registros
-- FROM T1 
-- GROUP BY ALL


, T2 as (
SELECT
cluster_id
, COUNT(DISTINCT(NUME_NIDE)) as IDs

, SAFE_DIVIDE(COUNT(DISTINCT(CASE WHEN reg_bandeuda > 0 THEN NUME_NIDE END)) , COUNT(DISTINCT(NUME_NIDE))) as porc_data_bandeuda
, SAFE_DIVIDE(SUM(CASE WHEN (reg_bandeuda = 0 or reg_bandeuda IS NULL) THEN 0 ELSE reg_bandeuda END) , COUNT(DISTINCT(NUME_NIDE))) as reg_prom_bandeuda
, SAFE_DIVIDE(SUM(CASE WHEN (cuentas_bandeuda = 0 or reg_bandeuda IS NULL) THEN 0 ELSE cuentas_bandeuda END) , COUNT(DISTINCT(NUME_NIDE))) as cuentas_prom_bandeuda
, MIN(min_periinfo_bandeuda) as min_periinfo_bandeuda
, MAX(max_periinfo_bandeuda) as max_periinfo_bandeuda

, SAFE_DIVIDE(COUNT(DISTINCT(CASE WHEN reg_tardeuda > 0 THEN NUME_NIDE END)) , COUNT(DISTINCT(NUME_NIDE))) as porc_data_tardeuda
, SAFE_DIVIDE(SUM(CASE WHEN (reg_tardeuda = 0 or reg_tardeuda IS NULL) THEN 0 ELSE reg_tardeuda END) , COUNT(DISTINCT(NUME_NIDE))) as reg_prom_tardeuda
, SAFE_DIVIDE(SUM(CASE WHEN (cuentas_tardeuda = 0 or cuentas_tardeuda IS NULL) THEN 0 ELSE cuentas_tardeuda END) , COUNT(DISTINCT(NUME_NIDE))) as cuentas_prom_tardeuda
, MIN(min_periinfo_tardeuda) as min_periinfo_tardeuda
, MAX(max_periinfo_tardeuda) as max_periinfo_tardeuda

, SAFE_DIVIDE(COUNT(DISTINCT(CASE WHEN reg_deucomer > 0 THEN NUME_NIDE END)) , COUNT(DISTINCT(NUME_NIDE))) as porc_data_deucomer
, SAFE_DIVIDE(SUM(CASE WHEN (reg_deucomer = 0 or reg_deucomer IS NULL) THEN 0 ELSE reg_deucomer END) , COUNT(DISTINCT(NUME_NIDE))) as reg_prom_deucomer
, SAFE_DIVIDE(SUM(CASE WHEN (cuentas_deucomer = 0 or cuentas_deucomer IS NULL) THEN 0 ELSE cuentas_deucomer END) , COUNT(DISTINCT(NUME_NIDE))) as cuentas_prom_deucomer
, MIN(min_periinfo_deucomer) as min_periinfo_deucomer
, MAX(max_periinfo_deucomer) as max_periinfo_deucomer
FROM T1
GROUP BY ALL
ORDER BY 2 DESC
)

-- , T3 as (
-- SELECT *
-- , ROW_NUMBER() OVER (ORDER BY IDs DESC) AS row_num
-- FROM T2
-- )

SELECT * FROM T2
