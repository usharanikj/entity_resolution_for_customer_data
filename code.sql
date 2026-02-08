-- CUSTOMER ENTITY RESOLUTION 
-- Purpose: Identify and group accounts that belong to the same customer into a single Customer ID.
DROP TABLE IF EXISTS raw_data;
CREATE TABLE raw_data (
    acct_id VARCHAR(50),
    fn VARCHAR(100),
    ln VARCHAR(100),
    dob DATE,
    email VARCHAR(150),
    phone VARCHAR(50),
    addr TEXT,
    gov_id VARCHAR(50)
);


SELECT COUNT(*) FROM raw_data;
SELECT COUNT(DISTINCT acct_id) FROM raw_data;

SELECT * FROM raw_data LIMIT 50;

-- 1. DATA STANDARDIZATION (PRE-PROCESSING)
-- Normalize fields to improve match rates: 
-- uppercase names, strip non-numeric characters from IDs/Phones, and extract ZIP codes for blocking.
DROP TABLE IF EXISTS stg_clean_accounts;
CREATE TABLE stg_clean_accounts AS
SELECT 
    acct_id,
    TRIM(UPPER(REGEXP_REPLACE(fn, '[^a-zA-Z ]', '', 'g'))) AS cfn,
    TRIM(UPPER(REGEXP_REPLACE(ln, '[^a-zA-Z ]', '', 'g'))) AS cln,
    dob,
    TRIM(LOWER(email)) AS cemail,
    RIGHT(REGEXP_REPLACE(phone, '[^0-9]', '', 'g'), 10) AS cphone,
    TRIM(UPPER(REGEXP_REPLACE(gov_id, '[^a-zA-Z0-9]', '', 'g'))) AS cid,
    TRIM(UPPER(REGEXP_REPLACE(addr, '[^a-zA-Z0-9 ]', '', 'g'))) AS caddr,
    RIGHT(REGEXP_REPLACE(addr, '[^0-9]', '', 'g'), 6) AS zip
FROM raw_data;

SELECT * FROM stg_clean_accounts LIMIT 50;

CREATE INDEX idx_stg_cid     ON stg_clean_accounts(cid);
CREATE INDEX idx_stg_phone  ON stg_clean_accounts(cphone);
CREATE INDEX idx_stg_email  ON stg_clean_accounts(cemail);
CREATE INDEX idx_stg_name_zip ON stg_clean_accounts(cfn, zip);
ANALYZE stg_clean_accounts;

 
-- 2. CANDIDATE SELECTION (BLOCKING)

-- Reduce the N^2 comparison problem by only pairing records that share at least 
-- one "strong" attribute (Gov ID, Phone, Email, or Name + ZIP).

-- Use UNION to force index usage

DROP TABLE IF EXISTS candidate_pairs;
CREATE TABLE candidate_pairs AS
WITH blocks AS (

    SELECT a.acct_id aid, b.acct_id bid
    FROM stg_clean_accounts a
    JOIN stg_clean_accounts b
        ON a.cid = b.cid
    WHERE a.acct_id < b.acct_id AND a.cid IS NOT NULL

    UNION
    SELECT a.acct_id, b.acct_id
    FROM stg_clean_accounts a
    JOIN stg_clean_accounts b
        ON a.cphone = b.cphone
    WHERE a.acct_id < b.acct_id AND a.cphone IS NOT NULL

    UNION
    SELECT a.acct_id, b.acct_id
    FROM stg_clean_accounts a
    JOIN stg_clean_accounts b
        ON a.cemail = b.cemail
    WHERE a.acct_id < b.acct_id AND a.cemail IS NOT NULL

    UNION
    SELECT a.acct_id, b.acct_id
    FROM stg_clean_accounts a
    JOIN stg_clean_accounts b
        ON LEFT(a.cfn, 3) = LEFT(b.cfn, 3)
       AND a.zip = b.zip
    WHERE a.acct_id < b.acct_id
)

SELECT
    bl.aid, bl.bid,
    a.cfn fn_a, b.cfn fn_b,
    a.cln ln_a, b.cln ln_b,
    a.dob dob_a, b.dob dob_b,
    a.cemail email_a, b.cemail email_b,
    a.cphone phone_a, b.cphone phone_b,
    a.caddr addr_a, b.caddr addr_b,
    a.cid id_a, b.cid id_b
FROM blocks bl
JOIN stg_clean_accounts a ON bl.aid = a.acct_id
JOIN stg_clean_accounts b ON bl.bid = b.acct_id;

SELECT * FROM candidate_pairs LIMIT 50;

-- Enable extension 
DROP EXTENSION IF EXISTS pg_trgm CASCADE;
CREATE EXTENSION pg_trgm;


-- Recommended for performance
CREATE INDEX idx_cfn_trgm   ON stg_clean_accounts USING gin (cfn gin_trgm_ops);
CREATE INDEX idx_cln_trgm   ON stg_clean_accounts USING gin (cln gin_trgm_ops);
CREATE INDEX idx_addr_trgm  ON stg_clean_accounts USING gin (caddr gin_trgm_ops);


-- 3. FUZZY SCORING & DECISION LOGIC
-- Calculate trigram similarity for names/addresses and apply business rules 
-- categorized by the "Modular Tier" framework for auditability.

DROP TABLE IF EXISTS final_matches;
CREATE TABLE final_matches AS
WITH scoring AS (
    SELECT *,
        similarity(fn_a, fn_b)  AS fn_score,
        similarity(ln_a, ln_b)  AS ln_score,
        similarity(addr_a, addr_b) AS addr_score,

        EXTRACT(YEAR FROM dob_a)::INT AS yob_a,
        EXTRACT(YEAR FROM dob_b)::INT AS yob_b,

        CONCAT(EXTRACT(YEAR FROM dob_a), '_', LEFT(fn_a,2)) AS yob_fn_a,
        CONCAT(EXTRACT(YEAR FROM dob_b), '_', LEFT(fn_b,2)) AS yob_fn_b
    FROM candidate_pairs
)

SELECT aid, bid,
CASE
    -- TIER 0: DETERMINISTIC & IDENTITY-LED (RULES 01-05)
    -- Highest confidence: Direct identity overlaps with name validation
    WHEN id_a = id_b AND fn_score > 0.80 THEN 'RULE_01_VERIFIED_ID'
    WHEN id_a = id_b AND fn_score > 0.70 AND ln_a = ln_b THEN 'RULE_02_VERIFIED_ID' 
    WHEN email_a = email_b AND phone_a = phone_b AND id_a = id_b THEN 'RULE_03_DIGITAL_TOKEN'
    WHEN email_a = email_b AND fn_score > 0.80 AND ln_score > 0.80 THEN 'RULE_04_DIGITAL_TOKEN'
    WHEN phone_a = phone_b AND fn_score > 0.80 AND ln_score > 0.80 THEN 'RULE_05_DIGITAL_TOKEN'

    -- TIER 1: DIGITAL TOKEN & ADDRESS CONFIRMATION (RULES 06-10)
    -- High confidence: Location overlap or cross-token validation
    WHEN addr_score > 0.75 
         AND fn_score > 0.80
         AND ln_score > 0.80
         AND (phone_a = phone_b OR email_a = email_b) THEN 'RULE_06_ADDRESS_MATCH'

    -- Catch-all for missing IDs using multi-token overlap
    WHEN (id_a IS NULL OR id_b IS NULL) AND email_a = email_b AND phone_a = phone_b AND ln_score > 0.70 THEN 'RULE_07_MISSING_ID_TOKEN'
    WHEN (id_a IS NULL OR id_b IS NULL) AND phone_a = phone_b AND fn_score > 0.80 AND ln_score > 0.80 THEN 'RULE_08_MISSING_ID_TOKEN'
    WHEN (id_a IS NULL OR id_b IS NULL) AND email_a = email_b AND fn_score > 0.80 AND ln_score > 0.80 THEN 'RULE_09_MISSING_ID_TOKEN'

    WHEN id_a = id_b AND (email_a = email_b OR phone_a = phone_b) THEN 'RULE_10_ID_PLUS_TOKEN'

    -- TIER 2: ERROR-TOLERANT & FUZZY FALLBACK (RULES 16-18)
    -- Moderate confidence: Handles data-entry typos and DOB variations
    -- Note: Rules 11-15 are reserved for future external data enhancement
    WHEN yob_fn_a = yob_fn_b
         AND ABS(yob_a - yob_b) <= 1
         AND fn_score > 0.75
         AND ln_score > 0.75 THEN 'RULE_16_DOB_FUZZY'

    WHEN ABS(yob_a - yob_b) = 0
         AND fn_score > 0.85
         AND ln_score > 0.85 THEN 'RULE_17_DOB_FUZZY'

    WHEN id_a = id_b AND (fn_score + ln_score)/2 > 0.80 THEN 'RULE_18_FUZZY_FALLBACK'

    ELSE 'NO_MATCH'
END AS match_decision
FROM scoring;

SELECT * FROM final_matches LIMIT 50;

-- 3. CLUSTERING EDGES
-- Build an adjacency list (edges) of all matched pairs.
DROP TABLE IF EXISTS edges;
CREATE TABLE edges AS
SELECT aid AS src, bid AS tgt FROM final_matches WHERE match_decision <> 'NO_MATCH'
UNION
SELECT bid, aid FROM final_matches WHERE match_decision <> 'NO_MATCH';

SELECT * FROM edges LIMIT 50;

-- 4. RECURSIVE CUSTOMER ID ASSIGNMENT
-- Use a Recursive CTE to group all connected nodes into a single cluster.
-- The lowest Acct_ID in a group becomes the "Golden" Customer_ID.
DROP TABLE IF EXISTS cust_clusters;
CREATE TABLE cust_clusters AS
WITH RECURSIVE traversal AS (
    SELECT acct_id AS node, acct_id AS parent
    FROM stg_clean_accounts

    UNION ALL
    SELECT t.node, e.tgt
    FROM traversal t
    JOIN edges e ON t.parent = e.src
    WHERE e.tgt < t.parent
)
SELECT node AS acct_id, MIN(parent) AS customer_id
FROM traversal
GROUP BY node;

SELECT * FROM cust_clusters LIMIT 50;

-- 5. ACCURACY REVIEW
-- Display clusters containing more than one record to verify match quality.
SELECT
    c.customer_id,
    s.acct_id,
    s.cfn, s.cln, s.dob,
    s.cphone, s.cemail, s.caddr, s.cid
FROM cust_clusters c
JOIN stg_clean_accounts s ON c.acct_id = s.acct_id
WHERE c.customer_id IN (
    SELECT customer_id
    FROM cust_clusters
    GROUP BY customer_id
    HAVING COUNT(*) > 1
    LIMIT 100
)
ORDER BY c.customer_id;