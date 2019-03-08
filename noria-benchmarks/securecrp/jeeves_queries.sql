--QUERY PaperList: SELECT Paper.*, LatestPaperVersion.title AS latest_version_title
--           FROM Paper
--           JOIN (SELECT *
--                 FROM PaperVersion
--		 GROUP BY PaperVersion.paper
--                 ORDER BY PaperVersion.time
--                 LIMIT 10)
--                AS LatestPaperVersion
--           ON (Paper.id = LatestPaperVersion.paper);

LatestPaperVersion: SELECT *
                 FROM PaperVersion
		 GROUP BY PaperVersion.paper
                 ORDER BY PaperVersion.time;
--                 LIMIT 10;

QUERY PaperList: SELECT Paper.*
           FROM Paper
	   JOIN LatestPaperVersion
	   ON (Paper.id = LatestPaperVersion.paper);
