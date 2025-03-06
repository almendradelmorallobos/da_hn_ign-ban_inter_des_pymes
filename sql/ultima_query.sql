SELECT * FROM `dfa-dna-ws0017-la-prd-2423.DS_INFO_LABORAL_HN.HN_TTEL_DIR_EXTERNA` ;

select RTN, count(distinct(RTN))  
from `dfa-dna-ws0017-la-prd-2423.DS_INFO_LABORAL_HN.HN_TTEL_DIR_EXTERNA`
group by 1
limit 100;

SELECT COUNT(DISTINCT(a.RTN))
FROM `dfa-dna-ws0017-la-prd-2423.DS_INFO_LABORAL_HN.HN_TTEL_DIR_EXTERNA` a
  LEFT JOIN (SELECT DISTINCT 
              numerortn14
             FROM `df-dna-dmt-la-prd-98d2.hn_cb_la_prd.general_hash_la_prd` a
             WHERE archive = '2025-01-15' ) b
    ON a.RTN = b.numerortn14
WHERE b.numerortn14 is not NULL
ORDER BY 1 DESC;
-- 20846

SELECT
    a.RTN,
    COUNT(DISTINCT a.NUME_NIDE) AS cantidad_numnide
FROM
    `dfa-dna-ws0017-la-prd-2423.DS_INFO_LABORAL_HN.HN_TTEL_DIR_EXTERNA` AS a
LEFT JOIN (
    SELECT DISTINCT numerortn14
    FROM `df-dna-dmt-la-prd-98d2.hn_cb_la_prd.general_hash_la_prd`
    WHERE archive = '2025-01-15'
) AS b ON a.RTN = b.numerortn14
WHERE b.numerortn14 IS NOT NULL
GROUP BY a.RTN  
ORDER BY a.RTN;


SELECT numerortn14, COUNT(DISTINCT nume_nide)
FROM `df-dna-dmt-la-prd-98d2.hn_cb_la_prd.general_hash_la_prd` 
WHERE archive = '2025-01-15'
GROUP BY numerortn14
order by 2 DESC;

SELECT COUNT(DISTINCT T1.NUME_NIDE)
FROM
    `dfa-dna-ws0017-la-prd-2423.DS_INFO_LABORAL_HN.HN_TTEL_DIR_EXTERNA` AS T1
    INNER JOIN `df-dna-dmt-la-prd-98d2.hn_cb_la_prd.general_hash_la_prd` AS T2 ON T1.RTN = T2.numerortn14
WHERE T2.archive = '2025-01-15';

SELECT a.RTN, b.numerortn14
FROM `dfa-dna-ws0017-la-prd-2423.DS_INFO_LABORAL_HN.HN_TTEL_DIR_EXTERNA` a
  LEFT JOIN (SELECT DISTINCT 
              numerortn14
             FROM `df-dna-dmt-la-prd-98d2.hn_cb_la_prd.general_hash_la_prd` a
             WHERE archive = '2025-01-15' ) b
    ON a.RTN = b.numerortn14
WHERE b.numerortn14 is NOT NULL
ORDER BY 1 DESC;

SELECT a.RTN, b.numerortn14
FROM `dfa-dna-ws0017-la-prd-2423.DS_INFO_LABORAL_HN.HN_TTEL_DIR_EXTERNA` a
  LEFT JOIN (SELECT DISTINCT 
              numerortn14
             FROM `df-dna-dmt-la-prd-98d2.hn_cb_la_prd.general_hash_la_prd` a
             WHERE archive = '2025-01-15' ) b
    ON a.RTN = b.numerortn14
WHERE b.numerortn14 is NOT NULL
ORDER BY 1 DESC;

SELECT COUNT(DISTINCT(a.RTN)), b.tipopersona
FROM `dfa-dna-ws0017-la-prd-2423.DS_INFO_LABORAL_HN.HN_TTEL_DIR_EXTERNA` a
  INNER JOIN (SELECT DISTINCT 
              numerortn14,
              tipopersona
             FROM `df-dna-dmt-la-prd-98d2.hn_cb_la_prd.general_hash_la_prd` a
             WHERE archive = '2025-01-15') b
    ON a.RTN = b.numerortn14
GROUP BY b.tipopersona
ORDER BY 1 DESC;