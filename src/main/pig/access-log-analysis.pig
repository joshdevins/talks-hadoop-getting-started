set job.name 'Apache Access Log Analysis'
set default_parallel 2

-- PiggyBank UDFs
DEFINE ApacheCombinedLogLoader org.apache.pig.piggybank.storage.apachelog.CombinedLogLoader();

raw = LOAD 'data/logs/*-access.log' USING ApacheCombinedLogLoader AS
    (remoteAddr, remoteLogname, user, timestamp, method, uri, protocol, statusCode, bytes, referrer, userAgent);

-- project just the fields that we want
logs = FOREACH raw GENERATE method, uri, statusCode;

-- determine the types of requests/responses that appear in the logs
-- group by all fields to find and count all permutations
grouped = GROUP logs BY (method, uri, statusCode);

-- count the permutations
permutations = FOREACH grouped GENERATE
    group.method AS method, group.uri AS uri, group.statusCode AS statusCode,
    COUNT(logs) AS count;
    
permutations = ORDER permutations BY count DESC;
    
STORE permutations INTO 'access-log-analysis' USING PigStorage(',');
