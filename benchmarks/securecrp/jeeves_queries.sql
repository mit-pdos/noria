LatestPaperVersion: SELECT * FROM PaperVersion ORDER BY PaperVersion.time LIMIT 1;
PaperList: SELECT Paper.*, title AS latest_version_title FROM Paper JOIN LatestPaperVersion ON (Paper.id = LatestPaperVersion.paper);
