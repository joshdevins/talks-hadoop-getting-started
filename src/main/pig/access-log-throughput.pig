set job.name 'Apache Access Log Throughput'
set default_parallel 2

%default local ''

$local REGISTER target/hadoop-getting-started-1-SNAPSHOT-jar-with-dependencies.jar

-- PiggyBank UDFs
DEFINE ApacheCombinedLogLoader org.apache.pig.piggybank.storage.apachelog.CombinedLogLoader();
DEFINE SUBSTRING org.apache.pig.piggybank.evaluation.string.SUBSTRING();
DEFINE MaxTupleBy1stField org.apache.pig.piggybank.evaluation.MaxTupleBy1stField();
DEFINE ROUND org.apache.pig.piggybank.evaluation.math.ROUND();

-- Custom UDFs
DEFINE SimpleDateTimeConverter net.joshdevins.talks.hadoopstart.pig.udf.evaluation.SimpleDateTimeConverter('dd/MMM/yyyy:HH:mm:ss Z', 'yyyy-MM-dd HH:mm:ss');
DEFINE Percentile net.joshdevins.talks.hadoopstart.pig.udf.evaluation.Percentile();

raw = LOAD 'data/logs/*-access.log' USING ApacheCombinedLogLoader AS
    (remoteAddr:chararray, remoteLogname:chararray, user:chararray,
     timestamp:chararray, method:chararray, uri:chararray, protocol:chararray,
     statusCode:int, bytes:long, referrer:chararray, userAgent:chararray);

-- project just the fields that we want
-- convert the timestamp to most significat fields first: 07/Sep/2010:10:17:45 +0000 -> 2010-09-07 10:17:45
logs = FOREACH raw GENERATE SimpleDateTimeConverter(timestamp) AS ts_second, method, statusCode;

-- let's only consider 2010 data, GET requests that return 2xx-3xx
logs = FILTER logs BY
    ts_second matches '2010-.*' AND method == 'GET' AND
    statusCode >= 200 AND statusCode <= 399;

-- generate the TPS by grouping by the timestamp in seconds, then counting
bySecond = GROUP logs BY ts_second;
bySecond = FOREACH bySecond GENERATE group AS ts_second, COUNT(logs) AS count;

-- values grouped by the hour
byHour = FOREACH bySecond GENERATE SUBSTRING(ts_second, 0, 13) AS ts_hour, count;

byHour = GROUP byHour BY ts_hour;
byHour = FOREACH byHour GENERATE group AS ts_hour, byHour.count AS counts;

-- generate statistics on TPS that occur in each hour
byHour = FOREACH byHour GENERATE
    CONCAT(ts_hour, ':00:00') AS ts_hour, SUM(counts) AS sum,
    ROUND(AVG(counts)) AS average, Percentile(counts, 50.0) AS median,
    Percentile(counts, 90.0) AS percentile_90, Percentile(counts, 99.0) AS percentile_99,
    FLATTEN(MaxTupleBy1stField(counts)) AS max;

-- order it by date and store it
-- schema: hour, sum, average, median, 90%, 99%, max
byHour = ORDER byHour BY ts_hour ASC;
STORE byHour INTO 'access-log-throughput' USING PigStorage(',');
