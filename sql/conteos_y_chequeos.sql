--- Exploración
-- Conteo RTN
SELECT
    RTN,  -- The column you want to count values from
    COUNT(*) AS count  -- Count the occurrences of each value
FROM
    `dfa-dna-ws0017-la-prd-2423.DS_INFO_LABORAL_HN.HN_TTEL_DIR_EXTERNA`
GROUP BY
    RTN  -- Group by the same column to count distinct values
ORDER BY
    count DESC;  -- Optional: Order the results by count in descending order

-- Conteo EMPRESA
SELECT
    EMPRESA,  -- Select the EMPRESA column
    COUNT(DISTINCT RAZON) AS count  -- Count the distinct RAZON values for each EMPRESA
FROM
    `dfa-dna-ws0017-la-prd-2423.DS_INFO_LABORAL_HN.HN_TTEL_DIR_EXTERNA`
GROUP BY
    EMPRESA  -- Group by EMPRESA to count distinct RAZON values within each company
ORDER BY
    count DESC;  -- Optional: Order the results by count in descending order

SELECT COUNT(DISTINCT EMPRESA) AS num_empresas
FROM `dfa-dna-ws0017-la-prd-2423.DS_INFO_LABORAL_HN.HN_TTEL_DIR_EXTERNA`;

SELECT COUNT(DISTINCT IDENTIDAD) AS num_empresas
FROM `dfa-dna-ws0017-la-prd-2423.DS_INFO_LABORAL_HN.HN_TTEL_DIR_EXTERNA`;

SELECT  -- The column you want to count values from
    COUNT(nume_nide) AS count  -- Count the occurrences of each value
FROM
    `dfa-dna-ws0017-la-prd-2423.DS_INFO_LABORAL_HN.HN_TTEL_DIR_EXTERNA`
 

SELECT
    RTN,
    nume_nide,  -- The column you want to count values from
    COUNT(*) AS count  -- Count the occurrences of each value
FROM
    `dfa-dna-ws0017-la-prd-2423.DS_INFO_LABORAL_HN.HN_TTEL_DIR_EXTERNA`
GROUP BY
    RTN  -- Group by the same column to count distinct values
ORDER BY
    count DESC; 
    
SELECT
    RTN,  -- The column you want to count values from
    COUNT(distinct razon) AS count  -- Count the occurrences of each value
FROM
    `dfa-dna-ws0017-la-prd-2423.DS_INFO_LABORAL_HN.HN_TTEL_DIR_EXTERNA`
GROUP BY
    RTN -- Group by the same column to count distinct values
ORDER BY
    count DESC;   

SELECT
    EMPRESA,  -- The column you want to count values from
    COUNT(*) AS count  -- Count the occurrences of each value
FROM
    `dfa-dna-ws0017-la-prd-2423.DS_INFO_LABORAL_HN.HN_TTEL_DIR_EXTERNA`
GROUP BY
    EMPRESA  -- Group by the same column to count distinct values
ORDER BY
    count DESC;  

SELECT
    NUME_NIDE,  -- The column you want to count values from
    COUNT(*) AS count  -- Count the occurrences of each value
FROM
    `dfa-dna-ws0017-la-prd-2423.DS_INFO_LABORAL_HN.HN_TTEL_DIR_EXTERNA`
GROUP BY
    NUME_NIDE  -- Group by the same column to count distinct values
ORDER BY
    count DESC; 


SELECT
    tipopersona,   -- Select the tipopersona column
    COUNT(distinct nume_nide) AS count  -- Count the occurrences of each tipopersona value
FROM (
    SELECT
        a.nume_nide,
        a.tipopersona
    FROM
        `df-dna-dmt-la-prd-98d2.hn_cb_decrypt_la_prd.general_la_prd` a
    WHERE archive = '2024-10-15'
    -- Other conditions (commented out)
)
GROUP BY
    tipopersona  -- Group by the tipopersona column to count distinct values
ORDER BY
    count DESC;  -- Optional: Order the results by count in descending order

SELECT
    a.tipopersona,
    count(distinct a.nume_nide) as contador
FROM
    `df-dna-dmt-la-prd-98d2.hn_cb_decrypt_la_prd.general_la_prd` a
WHERE archive = '2024-10-15'
GROUP BY
    a.tipopersona  -- Group by the tipopersona column to count distinct values
ORDER BY
    contador DESC;


SELECT
  a.nume_nide,
  a.numerotarjetaidentificacion,
  a.numeroidentificacion,
  a.numerortn14,
  a.numerortn7,
  a.nombrecompleto
FROM `df-dna-dmt-la-prd-98d2.hn_cb_decrypt_la_prd.general_la_prd` a, a.score b, a.deudcomerdet c
WHERE archive = '2024-10-15'
LIMIT 1000;



SELECT
    t1.tipopersona,  -- Select the tipopersona column
    COUNT(distinct t1.NUME_NIDE) AS count  -- Count the occurrences of each tipopersona value
FROM (
SELECT 
-- EMPRESA
-- -- , RAZON
-- , 
a.nume_nide
,a.tipopersona

FROM `df-dna-dmt-la-prd-98d2.hn_cb_la_prd.general_hash_la_prd` a
WHERE archive = '2024-10-15'  
-- WHERE a.NUME_NIDE = '2001823366'
-- WHERE EMPRESA = 'SECRETARIA DE EDUCACION PUBLICA'
AND a.tipopersona = 'EMP'
GROUP BY ALL
ORDER BY 1 DESC
) AS t1
JOIN `dfa-dna-ws0017-la-prd-2423.DS_INFO_LABORAL_HN.HN_TTEL_DIR_EXTERNA` AS t2
  ON t1.nume_nide = t2.NUME_NIDE
GROUP BY
    t1.tipopersona  -- Group by the tipopersona column to count distinct values
ORDER BY
    count DESC;  -- Optional: Order the results by count in descending order



SELECT 
    t2.tipopersona,  
    COUNT(DISTINCT t2.NUME_NIDE) AS count  
FROM (
SELECT
    NUME_NIDE 
FROM
    `dfa-dna-ws0017-la-prd-2423.DS_INFO_LABORAL_HN.HN_TTEL_DIR_EXTERNA`
GROUP BY ALL
) AS t1
LEFT JOIN `df-dna-dmt-la-prd-98d2.hn_cb_la_prd.general_hash_la_prd` AS t2
  ON t1.nume_nide = t2.NUME_NIDE
WHERE archive = '2024-10-15'   
GROUP BY
    t2.tipopersona  
ORDER BY
    count DESC; 

SELECT DISTINCT
a.tipopersona,
count(distinct a.nume_nide) as contador
FROM
`df-dna-dmt-la-prd-98d2.hn_cb_decrypt_la_prd.general_la_prd` a
WHERE archive = '2024-10-15'
GROUP BY
    a.tipopersona  
ORDER BY
    contador DESC