DROP DATABASE IF EXISTS mof_rpc;

CREATE DATABASE mof_rpc CHARACTER SET 'utf8' COLLATE 'utf8_general_ci';
USE mof_rpc;

CREATE TABLE docs (
   id          INT         NOT NULL AUTO_INCREMENT,
   doc_id      CHAR(8)     NOT NULL,
   title       CHAR(255)   NOT NULL,
   PRIMARY KEY (id)
)
