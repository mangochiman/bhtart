DROP TABLE IF EXISTS `moh_regimens`;
DROP TABLE IF EXISTS `moh_regimen_ingredient`;
DROP TABLE IF EXISTS `moh_regimen_doses`;
DROP TABLE IF EXISTS `moh_regimen_lookup`;

CREATE TABLE `moh_regimens` (
  `regimen_id` int(11)  NOT NULL AUTO_INCREMENT,
  `regimen_index` int(11) NOT NULL,
  `description` text DEFAULT NULL,
  `date_created` datetime DEFAULT NULL,
  `date_updated` datetime DEFAULT NULL,
  `creator` int(11) NOT NULL,
  `voided` tinyint(1) NOT NULL  DEFAULT '0',
  `voided_by` int(11) DEFAULT NULL,
   PRIMARY KEY (`regimen_id`)
);

CREATE TABLE `moh_regimen_doses` (
  `dose_id` int(11) NOT NULL AUTO_INCREMENT,
  `am` float(11)  DEFAULT NULL,
  `pm` float(11)  DEFAULT NULL,
  `date_created` datetime DEFAULT NULL,
  `date_updated` datetime DEFAULT NULL,
  `creator` int(11) DEFAULT NULL,
  `voided` tinyint(1) NOT NULL  DEFAULT '0',
  `voided_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`dose_id`)
);


CREATE TABLE `moh_regimen_ingredient` (
  `ingredient_id` int(11) NOT NULL AUTO_INCREMENT,
  `regimen_id` int(11)  REFERENCES moh_regimens (regimen_id),
  `drug_inventory_id` int(11)  REFERENCES drug (drug_id),
  `dose_id` int(11)  REFERENCES moh_regimen_doses (dose_id),
  `min_weight` float(11)  DEFAULT NULL,
  `max_weight` float(11)  DEFAULT NULL,
  `date_created` datetime DEFAULT NULL,
  `date_updated` datetime DEFAULT NULL,
  `creator` int(11) DEFAULT NULL,
  `voided` tinyint(1) NOT NULL  DEFAULT '0',
  `voided_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`ingredient_id`)
);

CREATE TABLE `moh_regimen_lookup` (
  `regimen_lookup_id` int(11) NOT NULL AUTO_INCREMENT,
  `regimen_id` int(11)  REFERENCES moh_regimens (regimen_id),
  `regimen_name` varchar(5)  NOT NULL,
  `drug_inventory_id` int(11)  REFERENCES drug (drug_id),
  `date_created` datetime DEFAULT NULL,
  `date_updated` datetime DEFAULT NULL,
  `creator` int(11) DEFAULT NULL,
  `voided` tinyint(1) NOT NULL  DEFAULT '0',
  `voided_by` int(11) DEFAULT NULL,
  PRIMARY KEY (`regimen_lookup_id`)
);



