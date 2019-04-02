--QUERY PaperList: SELECT Paper.*, LatestPaperVersion.title AS latest_version_title
--           FROM Paper
--           JOIN (SELECT *
--                 FROM PaperVersion
--		 GROUP BY PaperVersion.paper
--                 ORDER BY PaperVersion.time
--                 LIMIT 1)
--                AS LatestPaperVersion
--           ON (Paper.id = LatestPaperVersion.paper);

LatestPaperVersion: SELECT *
                 FROM PaperVersion
		 GROUP BY PaperVersion.paper
                 ORDER BY PaperVersion.time
                 LIMIT 1;

QUERY PaperList: SELECT Paper.*
           FROM Paper
--	   WHERE Paper.id = ?;
	   JOIN LatestPaperVersion
	   ON (Paper.id = LatestPaperVersion.paper);

--QUERY ReviewList: SELECT Review.*
--           FROM Review
--	   WHERE Review.paper = ?;
--	   JOIN Paper
--	   ON (Review.paper = Paper.id);
