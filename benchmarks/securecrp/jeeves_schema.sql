CREATE TABLE UserProfile (
    username varchar(1024),
    email varchar(1024),
    name varchar(1024),
    affiliation varchar(1024),
    acm_number varchar(1024),
    level varchar(12),
    PRIMARY KEY (email),
);

CREATE TABLE UserPCConflict (
    user varchar(1024),
    pc varchar(1024)
);

CREATE TABLE Paper (
    id int,
    author varchar(1024),
    accepted bool,
    PRIMARY KEY (id),
);

CREATE TABLE PaperPCConflict (
    paper int,
    pc varchar(1024),
);

CREATE TABLE PaperCoauthor (
    paper int,
    author varchar(1024),
);

CREATE TABLE ReviewAssignment (
    paper int,
    user varchar(1024),
    assign_type varchar(8),
);

CREATE TABLE PaperVersion (
    paper int,
    title varchar(1024),
    -- N.B. is of type "file" in Jeeves --
    contents varchar(1024),
    abstract text,
    time datetime DEFAULT CURRENT_TIMESTAMP,
);

CREATE TABLE Tag (
    name varchar(32)
    paper int,
);

CREATE TABLE Review (
    time datetime DEFAULT CURRENT_TIMESTAMP,
    paper int,
    reviewer varchar(1024),
    contents text,
    score_novelty int,
    score_presentation int,
    score_technical int,
    score_confidence int,
);

CREATE TABLE Comment (
    time datetime DEFAULT CURRENT_TIMESTAMP,
    paper int,
    user varchar(1024),
    contents text,
);
