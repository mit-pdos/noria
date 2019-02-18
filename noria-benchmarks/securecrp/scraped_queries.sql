--GET /register:
--QUERY user: SELECT  auth_user.id,   auth_user.password,   auth_user.last_login,   auth_user.is_superuser,   auth_user.username,   auth_user.first_name,   auth_user.last_name,   auth_user.email,   auth_user.is_staff,   auth_user.is_active,   auth_user.date_joined  FROM  auth_user, UserContext  WHERE  auth_user.username  = UserContext.id
--SELECT (1) AS  a  FROM  auth_user  WHERE  auth_user.username  = UserContext.id  LIMIT 1
--SELECT  auth_user.id,   auth_user.password,   auth_user.last_login,   auth_user.is_superuser,   auth_user.username,   auth_user.first_name,   auth_user.last_name,   auth_user.email,   auth_user.is_staff,   auth_user.is_active,   auth_user.date_joined  FROM  auth_user  WHERE  auth_user.username  = 'alana'
--SELECT (1) AS  a  FROM  django_session  WHERE  django_session.session_key  = 'cndj7j0oknuy4ztpjyxa0kkipc452qs3'  LIMIT 100
--SELECT  django_session.session_key,   django_session.session_data,   django_session.expire_date  FROM  django_session  WHERE  django_session.session_key  = 'cndj7j0oknuy4ztpjyxa0kkipc452qs3'

--GET /index:
--SELECT  django_session.session_key,   django_session.session_data,   django_session.expire_date  FROM  django_session  WHERE ( django_session.session_key  = 'cndj7j0oknuy4ztpjyxa0kkipc452qs3'  AND  django_session.expire_date  > '2019-01-30 01:18:11' )
--SELECT  auth_user.id,   auth_user.password,   auth_user.last_login,   auth_user.is_superuser,   auth_user.username,   auth_user.first_name,   auth_user.last_name,   auth_user.email,   auth_user.is_staff,   auth_user.is_active,   auth_user.date_joined  FROM  auth_user  WHERE  auth_user.id  = 1
QUERY Users: SELECT * FROM UserProfile WHERE UserProfile.username  = ?;
QUERY AcceptedPapers: SELECT * FROM Paper;
QUERY PaperList: SELECT Paper.*, LatestPaperVersion.title AS latest_version_title
           FROM Paper
           JOIN (SELECT *
                 FROM PaperVersion
                 ORDER BY PaperVersion.time
                 LIMIT 1)
                AS LatestPaperVersion
           ON (Paper.id = LatestPaperVersion.paper);
