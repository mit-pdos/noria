--QUERY PaperList: SELECT Paper.*, LatestPaperVersion.title AS latest_version_title
--           FROM Paper
--           JOIN (SELECT *
--                 FROM PaperVersion
--		 GROUP BY PaperVersion.paper
--                 ORDER BY PaperVersion.time
--                 LIMIT 10)
--                AS LatestPaperVersion
--           ON (Paper.id = LatestPaperVersion.paper);

QUERY PaperList: SELECT Paper.*
           FROM Paper;
