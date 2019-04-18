QUERY ReviewList: SELECT Paper.id, Review.reviewer, Review.contents, Review.score_novelty, Review.score_presentation, Review.score_technical, Review.score_confidence, Paper.author as author
           FROM Paper
--	   WHERE Paper.id = ?
	   JOIN Review
	   ON (Paper.id = Review.paper);

-- Below query is broken because a Union is attempted with two tables that use "id" as column name and one table that uses "paper" (but has id as an alias), and checking for matching columns is done by name (without reference to aliases).
--QUERY ReviewList: SELECT Paper.id, Review.reviewer, Review.contents, Review.score_novelty, Review.score_presentation, Review.score_technical, Review.score_confidence, Paper.author as author
--           FROM Review
--	   JOIN Paper
--	   ON (Paper.id = Review.paper);
