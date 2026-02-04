-- Raw data
CREATE TABLE raw_data (
    acct_id VARCHAR(50),
    fn VARCHAR(100),
    ln VARCHAR(100),
    email VARCHAR(150),
    phone VARCHAR(50),
    addr TEXT,
    gov_id VARCHAR(50)
);

-- Import CSV
SELECT COUNT(*) FROM raw_data; -- Should return 1,000,000

-- Sample data (due to lack of disk space)
CREATE TABLE raw_data_sample AS
SELECT * FROM raw_data
ORDER BY random() LIMIT 100000; 

-- 1. CLEANING & STANDARDIZATION
CREATE TABLE stg_clean_accounts AS
SELECT 
    acct_id, 
    TRIM(UPPER(REGEXP_REPLACE(fn, '[^a-zA-Z ]', '', 'g'))) AS cfn, 
    TRIM(UPPER(REGEXP_REPLACE(ln, '[^a-zA-Z ]', '', 'g'))) AS cln, 
    TRIM(LOWER(email)) AS cemail,
    RIGHT(REGEXP_REPLACE(phone, '[^0-9]', '', 'g'), 10) AS cphone,
    TRIM(UPPER(REGEXP_REPLACE(gov_id, '[^a-zA-Z0-9]', '', 'g'))) AS cid, 
    TRIM(UPPER(REGEXP_REPLACE(addr, '[^a-zA-Z0-9 ]', '', 'g'))) AS caddr,
    RIGHT(TRIM(addr), 6) AS zip
FROM raw_data_sample;

-- Recommended for performance
CREATE INDEX idx_stg_cid ON stg_clean_accounts(cid);
CREATE INDEX idx_stg_phone ON stg_clean_accounts(cphone);
CREATE INDEX idx_stg_email ON stg_clean_accounts(cemail);
CREATE INDEX idx_stg_name_zip ON stg_clean_accounts(cfn, zip);

ANALYZE stg_clean_accounts;

-- 2. CANDIDATE SELECTION (BLOCKING)
-- Using UNION to force index usage
CREATE TABLE candidate_pairs AS
WITH blocks AS (
    SELECT a.acct_id AS aid, b.acct_id AS bid FROM stg_clean_accounts a 
    JOIN stg_clean_accounts b ON a.cid = b.cid WHERE a.acct_id < b.acct_id AND a.cid IS NOT NULL
    UNION
    SELECT a.acct_id AS aid, b.acct_id AS bid FROM stg_clean_accounts a 
    JOIN stg_clean_accounts b ON a.cphone = b.cphone WHERE a.acct_id < b.acct_id AND a.cphone IS NOT NULL
    UNION
    SELECT a.acct_id AS aid, b.acct_id AS bid FROM stg_clean_accounts a 
    JOIN stg_clean_accounts b ON a.cemail = b.cemail WHERE a.acct_id < b.acct_id AND a.cemail IS NOT NULL
    UNION
    SELECT a.acct_id AS aid, b.acct_id AS bid FROM stg_clean_accounts a 
    JOIN stg_clean_accounts b ON LEFT(a.cfn,3) = LEFT(b.cfn,3) AND a.zip = b.zip WHERE a.acct_id < b.acct_id
)
SELECT 
    bl.aid, bl.bid,
    a.cfn AS fn_a, b.cfn AS fn_b, a.cln AS ln_a, b.cln AS ln_b,
    a.cemail AS email_a, b.cemail AS email_b,
    a.cphone AS phone_a, b.cphone AS phone_b,
    a.caddr AS addr_a, b.caddr AS addr_b,
    a.cid AS id_a, b.cid AS id_b
FROM blocks bl
JOIN stg_clean_accounts a ON bl.aid = a.acct_id
JOIN stg_clean_accounts b ON bl.bid = b.acct_id;

-- Enable extension 
DROP EXTENSION IF EXISTS pg_trgm CASCADE;
CREATE EXTENSION pg_trgm;

-- Recommended for performance
CREATE INDEX idx_stg_cfn_trgm ON stg_clean_accounts USING gin (cfn gin_trgm_ops);
CREATE INDEX idx_stg_cln_trgm ON stg_clean_accounts USING gin (cln gin_trgm_ops);
CREATE INDEX idx_stg_addr_trgm ON stg_clean_accounts USING gin (caddr gin_trgm_ops);

-- 3. FINAL MATCH SCORING
CREATE TABLE final_matches AS
WITH scoring AS (
    SELECT *,
        similarity(fn_a, fn_b) AS fn_score,
        similarity(ln_a, ln_b) AS ln_score,
        similarity(addr_a, addr_b) AS addr_score
    FROM candidate_pairs
)
SELECT aid, bid,
CASE 
    -- 1. VERIFIED IDENTITY (ID + Name confirmation)
    WHEN id_a = id_b AND fn_score > 0.75 THEN 'RULE_01'
    WHEN id_a = id_b AND ln_a = ln_b THEN 'RULE_02'

    -- 2. DIGITAL TOKENS
    WHEN email_a = email_b AND phone_a = phone_b AND id_a = id_b THEN 'RULE_03'
    WHEN email_a = email_b AND fn_score > 0.80 AND ln_score > 0.80 THEN 'RULE_04'
    WHEN phone_a = phone_b AND fn_score > 0.80 AND ln_score > 0.80 THEN 'RULE_05'

    -- 3. ANTI-OFFICE
    WHEN addr_score > 0.75 
         AND fn_score > 0.80 
         AND ln_score > 0.80
         AND (phone_a = phone_b OR email_a = email_b) THEN 'RULE_06'

    -- 4. MISSING GOVT ID
    WHEN id_a IS NULL AND email_a = email_b AND phone_a = phone_b AND ln_score > 0.70 THEN 'RULE_07'
    WHEN id_a IS NULL AND phone_a = phone_b AND fn_score > 0.80 AND ln_score > 0.80 THEN 'RULE_08'
    WHEN id_a IS NULL AND email_a = email_b AND fn_score > 0.80 AND ln_score > 0.80 THEN 'RULE_09'

    -- 5. UNIQUE IDENTIFIER + ONE CONTACT
    WHEN id_a = id_b AND (email_a = email_b OR phone_a = phone_b) THEN 'RULE_10'

    -- 6. FUZZY MATCHES
    WHEN fn_score BETWEEN 0.65 AND 0.80 AND ln_a = ln_b AND id_a = id_b THEN 'RULE_11'
    WHEN fn_score > 0.80 AND ln_score > 0.80 AND email_a = email_b AND addr_score > 0.60 THEN 'RULE_12'
    WHEN id_a = id_b AND addr_score > 0.80 AND ln_score > 0.70 THEN 'RULE_13'
    WHEN phone_a = phone_b AND email_a = email_b AND addr_score > 0.70 THEN 'RULE_14'
    WHEN id_a = id_b AND (fn_score + ln_score)/2 > 0.80 THEN 'RULE_15'

    ELSE 'NO_MATCH'
END AS match_decision
FROM scoring;

-- 3. CLUSTERING EDGES
CREATE TABLE edges AS
SELECT aid AS src, bid AS tgt FROM final_matches WHERE match_decision != 'NO_MATCH'
UNION
SELECT bid AS src, aid AS tgt FROM final_matches WHERE match_decision != 'NO_MATCH';

-- 4. RECURSIVE CUSTOMER ID ASSIGNMENT
CREATE TABLE cust_clusters AS
WITH RECURSIVE traversal AS (
    SELECT 
        acct_id AS node, 
        acct_id AS parent
    FROM stg_clean_accounts
    UNION ALL
    SELECT 
        t.node, 
        e.tgt AS parent
    FROM traversal t
    JOIN edges e ON t.parent = e.src
    WHERE e.tgt < t.parent 
)
SELECT 
    node AS acct_id, 
    MIN(parent) AS customer_id 
FROM traversal
GROUP BY node;

-- 5. ACCURACY REVIEW
SELECT 
    c.customer_id,
    s.acct_id,
    s.cfn, s.cln, s.cphone, s.cemail, s.caddr, s.cid
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