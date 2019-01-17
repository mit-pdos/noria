QUERY posts: select * from Post where p_cid = ?;
-- QUERY posts: select p_cid, count(*) from Post where p_cid = ? GROUP BY p_cid;
-- QUERY posts: select p_author, count(p_id) from Post group by p_author;
