-- QUERY posts: select * from Post where p_cid=?;
QUERY post_count: select count(p_id) from Post group by p_cid;
