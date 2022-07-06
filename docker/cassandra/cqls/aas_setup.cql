USE ecloud_aas;

CREATE TABLE users(
                    username varchar,
                    password varchar,
                    roles set<text>,
                    locked boolean,
                    PRIMARY KEY (username));
 
CREATE TABLE aois (
                    id varchar,
                    objId varchar,
                    objClass varchar,
                    isInheriting boolean,
                    owner varchar,
                    isOwnerPrincipal boolean,
                    parentObjId varchar,
                    parentObjClass varchar,
                    PRIMARY KEY (id));
CREATE TABLE children (
                    id varchar,
                    childId varchar,
                    objId varchar,
                    objClass varchar,
                    PRIMARY KEY (id, childId));
CREATE TABLE acls (
                    id varchar,
                    sid varchar,
                    aclOrder int,
                    mask int,
                    isSidPrincipal boolean,
                    isGranting boolean,
                    isAuditSuccess boolean,
                    isAuditFailure boolean,
                    PRIMARY KEY (id, sid, aclOrder));