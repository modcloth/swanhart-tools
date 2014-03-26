LOAD DATA LOCAL INFILE 'schema/errors.csv'
	INTO TABLE `fv_condition`
	CHARACTER SET utf8
	COLUMNS
		TERMINATED BY ','
		OPTIONALLY ENCLOSED BY '"'
		ESCAPED BY '\\'
	LINES TERMINATED BY '\n'
	IGNORE 1 LINES
	(`code`, `level`, `message`);

