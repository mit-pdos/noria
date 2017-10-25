posts: select * from Post where p_cid=?;
public_posts: select * from Post WHERE Post.p_private=0 and p_cid=?;
private_posts: select * from Post WHERE Post.p_private=1 and p_cid=?;
