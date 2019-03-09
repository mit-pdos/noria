CREATE TABLE UserProfile (
    username varchar(1024),
    email varchar(1024),
    name varchar(1024),
    affiliation varchar(1024),
    acm_number varchar(1024),
    level varchar(12),
    PRIMARY KEY (username)
);

CREATE TABLE Paper (
    id int,
    author varchar(1024),
    accepted tinyint(1),
    PRIMARY KEY (id)
);

CREATE TABLE PaperVersion (
    paper int,
    title varchar(1024),
    -- N.B. is of type "file" in Jeeves --
    contents varchar(1024),
    abstract text,
    time datetime DEFAULT CURRENT_TIMESTAMP,
    KEY paper (paper)
);
