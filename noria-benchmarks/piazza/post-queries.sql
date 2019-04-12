-- QUERY posts: select * from Post where p_cid = ?;
-- QUERY post_count: select p_cid, count(p_id) from Post where p_cid = ? group by p_cid;
--
--
--
QUERY posts: select * from Post where p_cid = ?;
CREATE VIEW post_count_agg: select p_cid, count(p_id) as p_cnt from Post group by p_cid;
QUERY post_count: select p_cid, p_cnt from shallow_post_count_agg where p_cid = ?;
