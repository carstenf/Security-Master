import mysql.connector

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="root")

database_name='securities_master'    


if __name__ == '__main__':

    query = """

            -- MySQL Workbench Forward Engineering

        SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
        SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
        SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

        CREATE SCHEMA IF NOT EXISTS `{}` DEFAULT CHARACTER SET utf8 ;
        USE `{}` ;

        -- -----------------------------------------------------
        -- Table asset_class
        -- -----------------------------------------------------
        CREATE TABLE IF NOT EXISTS `{}`.`asset_class` (
        `id` INT(11) NOT NULL AUTO_INCREMENT,
        `asset_class` VARCHAR(255) NOT NULL,
        PRIMARY KEY (`id`),
        UNIQUE INDEX `id_asset_class_UNIQUE` (`id` ASC) VISIBLE,
        UNIQUE INDEX `asset_class_UNIQUE` (`asset_class` ASC) VISIBLE)
        ENGINE = InnoDB
        DEFAULT CHARACTER SET = utf8;


        -- -----------------------------------------------------
        -- Table data_vendor
        -- -----------------------------------------------------
        CREATE TABLE IF NOT EXISTS `{}`.`data_vendor` (
        `id` INT(11) NOT NULL AUTO_INCREMENT,
        `name` VARCHAR(32) NOT NULL,
        `website_url` VARCHAR(255) NULL DEFAULT NULL,
        PRIMARY KEY (`id`),
        UNIQUE INDEX `id_vendor_UNIQUE` (`id` ASC) VISIBLE,
        UNIQUE INDEX `name_UNIQUE` (`name` ASC) VISIBLE)
        ENGINE = InnoDB
        AUTO_INCREMENT = 3
        DEFAULT CHARACTER SET = utf8;


        -- -----------------------------------------------------
        -- Table  exchange
        -- -----------------------------------------------------
        CREATE TABLE IF NOT EXISTS `{}`.`exchange` (
        `id` INT(11) NOT NULL AUTO_INCREMENT,
        `name` VARCHAR(10) NOT NULL,
        `currency` CHAR(10) NULL DEFAULT NULL,
        PRIMARY KEY (`id`),
        UNIQUE INDEX `id_UNIQUE` (`id` ASC) VISIBLE,
        UNIQUE INDEX `name_UNIQUE` (`name` ASC) VISIBLE)
        ENGINE = InnoDB
        DEFAULT CHARACTER SET = utf8;


        -- -----------------------------------------------------
        -- Table `security`
        -- -----------------------------------------------------
        CREATE TABLE IF NOT EXISTS `{}`.`security` (
        `id` INT(11) NOT NULL AUTO_INCREMENT,
        `ticker` VARCHAR(10) NULL DEFAULT NULL,
        `name` VARCHAR(255) NULL DEFAULT NULL,
        `code` DECIMAL(10) NULL DEFAULT NULL,
        `sector` VARCHAR(255) NULL DEFAULT NULL,
        `isdelisted` VARCHAR(3) NULL DEFAULT NULL,
        `ttable` VARCHAR(8) NULL DEFAULT NULL,
        `category` VARCHAR(45) NULL DEFAULT NULL,
        `exchange_id` INT(11) NOT NULL,
        `asset_class_id` INT(11) NOT NULL,
        `data_vendor_id` INT(11) NOT NULL,
        PRIMARY KEY (`id`),
        UNIQUE INDEX `id_security_UNIQUE` (`id` ASC) VISIBLE,
        UNIQUE INDEX `vendor_tik_table` (`ticker` ASC, `data_vendor_id` ASC, `ttable` ASC) VISIBLE,
        INDEX `ticker` (`ticker` ASC) VISIBLE,
        INDEX `fk_security_exchange1_idx` (`exchange_id` ASC) VISIBLE,
        INDEX `fk_security_asset_class1_idx` (`asset_class_id` ASC) VISIBLE,
        INDEX `fk_security_data_vendor1_idx` (`data_vendor_id` ASC) VISIBLE,
        CONSTRAINT `fk_security_asset_class1`
            FOREIGN KEY (`asset_class_id`)
            REFERENCES `{}`.`asset_class` (`id`),
        CONSTRAINT `fk_security_data_vendor1`
            FOREIGN KEY (`data_vendor_id`)
            REFERENCES `{}`.`data_vendor` (`id`),
        CONSTRAINT `fk_security_exchange1`
            FOREIGN KEY (`exchange_id`)
            REFERENCES `{}`.`exchange` (`id`))
        ENGINE = InnoDB
        DEFAULT CHARACTER SET = utf8;


        -- -----------------------------------------------------
        -- Table `SP500_const`
        -- -----------------------------------------------------
        CREATE TABLE IF NOT EXISTS `{}`.`SP500_const` (
        `id` INT(11) NOT NULL AUTO_INCREMENT,
        `date` DATE NOT NULL,
        `action` VARCHAR(10) NULL DEFAULT NULL,
        `ticker` VARCHAR(10) NULL,
        `contraticker` VARCHAR(10) NULL DEFAULT NULL,
        `security_id` INT(11) NOT NULL,
        PRIMARY KEY (`id`),
        INDEX `fk_SP500_const_security1_idx` (`security_id` ASC) VISIBLE,
        UNIQUE INDEX `id_UNIQUE` (`id` ASC) VISIBLE,
        UNIQUE INDEX `sec_id_date` (`security_id` ASC, `date` ASC) VISIBLE,
        CONSTRAINT `fk_SP500_const_security1`
            FOREIGN KEY (`security_id`)
            REFERENCES `{}`.`security` (`id`))
        ENGINE = InnoDB
        DEFAULT CHARACTER SET = utf8;


        -- -----------------------------------------------------
        -- Table `corp_action`
        -- -----------------------------------------------------
        CREATE TABLE IF NOT EXISTS `{}`.`corp_action` (
        `id` INT(11) NOT NULL AUTO_INCREMENT,
        `date` DATE NOT NULL,
        `action` VARCHAR(30) NULL DEFAULT NULL,
        `value` DOUBLE NULL DEFAULT NULL,
        `contraticker` VARCHAR(10) NULL DEFAULT NULL,
        `security_id` INT(11) NOT NULL,
        PRIMARY KEY (`id`),
        UNIQUE INDEX `sec_id_date` (`date` ASC, `security_id` ASC, `action` ASC) VISIBLE,
        INDEX `fk_corp_action_security1_idx` (`security_id` ASC) VISIBLE,
        UNIQUE INDEX `id_UNIQUE` (`id` ASC) VISIBLE,
        CONSTRAINT `fk_corp_action_security1`
            FOREIGN KEY (`security_id`)
            REFERENCES `{}`.`security` (`id`))
        ENGINE = InnoDB
        DEFAULT CHARACTER SET = utf8;


        -- -----------------------------------------------------
        -- Table `daily_price`
        -- -----------------------------------------------------
        CREATE TABLE IF NOT EXISTS `{}`.`daily_price` (
        `id` INT(11) NOT NULL AUTO_INCREMENT,
        `trade_date` DATE NOT NULL,
        `open` DOUBLE UNSIGNED NULL DEFAULT NULL,
        `high` DOUBLE NULL DEFAULT NULL,
        `low` DOUBLE NULL DEFAULT NULL,
        `close` DOUBLE NULL DEFAULT NULL,
        `adj_close` DOUBLE NULL DEFAULT NULL,
        `volume` DOUBLE NULL DEFAULT NULL,
        `security_id` INT(11) NOT NULL,
        PRIMARY KEY (`id`),
        UNIQUE INDEX `id_price_UNIQUE` (`id` ASC) VISIBLE,
        UNIQUE INDEX `date_sec_id` (`trade_date` ASC, `security_id` ASC) VISIBLE,
        INDEX `price_date` (`trade_date` ASC) VISIBLE,
        INDEX `fk_daily_price_security1_idx` (`security_id` ASC) VISIBLE,
        CONSTRAINT `fk_daily_price_security1`
            FOREIGN KEY (`security_id`)
            REFERENCES `{}`.`security` (`id`))
        ENGINE = InnoDB
        DEFAULT CHARACTER SET = utf8;


        -- -----------------------------------------------------
        -- Table `dividends`
        -- -----------------------------------------------------
        CREATE TABLE IF NOT EXISTS `{}`.`dividends` (
        `id` INT(11) NOT NULL AUTO_INCREMENT,
        `date` DATE NOT NULL,
        `dividends` DOUBLE NULL DEFAULT NULL,
        `security_id` INT(11) NOT NULL,
        PRIMARY KEY (`id`),
        UNIQUE INDEX `sec_id_date` (`date` ASC, `security_id` ASC) VISIBLE,
        INDEX `fk_dividents_security1_idx` (`security_id` ASC) VISIBLE,
        UNIQUE INDEX `id_UNIQUE` (`id` ASC) VISIBLE,
        CONSTRAINT `fk_dividents_security1`
            FOREIGN KEY (`security_id`)
            REFERENCES `{}`.`security` (`id`))
        ENGINE = InnoDB
        DEFAULT CHARACTER SET = utf8;


        -- -----------------------------------------------------
        -- Table `fundamental`
        -- -----------------------------------------------------
        CREATE TABLE IF NOT EXISTS `{}`.`fundamental` (
        `id` INT(11) NOT NULL AUTO_INCREMENT,
        `revenue` DOUBLE NULL DEFAULT NULL,
        `cor` DOUBLE NULL DEFAULT NULL,
        `sgna` DOUBLE NULL DEFAULT NULL,
        `rnd` DOUBLE NULL DEFAULT NULL,
        `opex` DOUBLE NULL DEFAULT NULL,
        `intexp` DOUBLE NULL DEFAULT NULL,
        `taxexp` DOUBLE NULL DEFAULT NULL,
        `netincdis` DOUBLE NULL DEFAULT NULL,
        `consolinc` DOUBLE NULL DEFAULT NULL,
        `netincnci` DOUBLE NULL DEFAULT NULL,
        `netinc` DOUBLE NULL DEFAULT NULL,
        `prefdivis` DOUBLE NULL DEFAULT NULL,
        `netinccmn` DOUBLE NULL DEFAULT NULL,
        `eps` DOUBLE NULL DEFAULT NULL,
        `epsdil` DOUBLE NULL DEFAULT NULL,
        `shareswa` DOUBLE NULL DEFAULT NULL,
        `shareswadil` DOUBLE NULL DEFAULT NULL,
        `capex` DOUBLE NULL DEFAULT NULL,
        `ncfbus` DOUBLE NULL DEFAULT NULL,
        `ncfinv` DOUBLE NULL DEFAULT NULL,
        `ncff` DOUBLE NULL DEFAULT NULL,
        `ncfdebt` DOUBLE NULL DEFAULT NULL,
        `ncfcommon` DOUBLE NULL DEFAULT NULL,
        `ncfdiv` DOUBLE NULL DEFAULT NULL,
        `ncfi` DOUBLE NULL DEFAULT NULL,
        `ncfo` DOUBLE NULL DEFAULT NULL,
        `ncfx` DOUBLE NULL DEFAULT NULL,
        `ncf` DOUBLE NULL DEFAULT NULL,
        `sbcomp` DOUBLE NULL DEFAULT NULL,
        `depamor` DOUBLE NULL DEFAULT NULL,
        `assets` DOUBLE NULL DEFAULT NULL,
        `cashneq` DOUBLE NULL DEFAULT NULL,
        `investments` DOUBLE NULL DEFAULT NULL,
        `investmentsc` DOUBLE NULL DEFAULT NULL,
        `investmentsnc` DOUBLE NULL DEFAULT NULL,
        `deferredrev` DOUBLE NULL DEFAULT NULL,
        `deposits` DOUBLE NULL DEFAULT NULL,
        `ppnenet` DOUBLE NULL DEFAULT NULL,
        `inventory` DOUBLE NULL DEFAULT NULL,
        `taxassets` DOUBLE NULL DEFAULT NULL,
        `receivables` DOUBLE NULL DEFAULT NULL,
        `payables` DOUBLE NULL DEFAULT NULL,
        `intangibles` DOUBLE NULL DEFAULT NULL,
        `liabilities` DOUBLE NULL DEFAULT NULL,
        `equity` DOUBLE NULL DEFAULT NULL,
        `retearn` DOUBLE NULL DEFAULT NULL,
        `accoci` DOUBLE NULL DEFAULT NULL,
        `assetsc` DOUBLE NULL DEFAULT NULL,
        `assetsnc` DOUBLE NULL DEFAULT NULL,
        `liabilitiesc` DOUBLE NULL DEFAULT NULL,
        `liabilitiesnc` DOUBLE NULL DEFAULT NULL,
        `taxliabilities` DOUBLE NULL DEFAULT NULL,
        `debt` DOUBLE NULL DEFAULT NULL,
        `debtc` DOUBLE NULL DEFAULT NULL,
        `debtnc` DOUBLE NULL DEFAULT NULL,
        `ebt` DOUBLE NULL DEFAULT NULL,
        `ebit` DOUBLE NULL DEFAULT NULL,
        `ebitda` DOUBLE NULL DEFAULT NULL,
        `fxusd` DOUBLE NULL DEFAULT NULL,
        `equityusd` DOUBLE NULL DEFAULT NULL,
        `epsusd` DOUBLE NULL DEFAULT NULL,
        `revenueusd` DOUBLE NULL DEFAULT NULL,
        `netinccmnusd` DOUBLE NULL DEFAULT NULL,
        `cashnequsd` DOUBLE NULL DEFAULT NULL,
        `debtusd` DOUBLE NULL DEFAULT NULL,
        `ebitusd` DOUBLE NULL DEFAULT NULL,
        `ebitdausd` DOUBLE NULL DEFAULT NULL,
        `sharesbas` DOUBLE NULL DEFAULT NULL,
        `dps` DOUBLE NULL DEFAULT NULL,
        `sharefactor` DOUBLE NULL DEFAULT NULL,
        `marketcap` DOUBLE NULL DEFAULT NULL,
        `ev` DOUBLE NULL DEFAULT NULL,
        `invcap` DOUBLE NULL DEFAULT NULL,
        `equityavg` DOUBLE NULL DEFAULT NULL,
        `assetsavg` DOUBLE NULL DEFAULT NULL,
        `invcapavg` DOUBLE NULL DEFAULT NULL,
        `tangibles` DOUBLE NULL DEFAULT NULL,
        `roe` DOUBLE NULL DEFAULT NULL,
        `roa` DOUBLE NULL DEFAULT NULL,
        `fcf` DOUBLE NULL DEFAULT NULL,
        `roic` DOUBLE NULL DEFAULT NULL,
        `gp` DOUBLE NULL DEFAULT NULL,
        `opinc` DOUBLE NULL DEFAULT NULL,
        `grossmargin` DOUBLE NULL DEFAULT NULL,
        `netmargin` DOUBLE NULL DEFAULT NULL,
        `ebitdamargin` DOUBLE NULL DEFAULT NULL,
        `ros` DOUBLE NULL DEFAULT NULL,
        `assetturnover` DOUBLE NULL DEFAULT NULL,
        `payoutratio` DOUBLE NULL DEFAULT NULL,
        `evebitda` DOUBLE NULL DEFAULT NULL,
        `evebit` DOUBLE NULL DEFAULT NULL,
        `pe` DOUBLE NULL DEFAULT NULL,
        `pe1` DOUBLE NULL DEFAULT NULL,
        `sps` DOUBLE NULL DEFAULT NULL,
        `ps1` DOUBLE NULL DEFAULT NULL,
        `ps` DOUBLE NULL DEFAULT NULL,
        `pb` DOUBLE NULL DEFAULT NULL,
        `de` DOUBLE NULL DEFAULT NULL,
        `divyield` DOUBLE NULL DEFAULT NULL,
        `currentratio` DOUBLE NULL DEFAULT NULL,
        `workingcapital` DOUBLE NULL DEFAULT NULL,
        `fcfps` DOUBLE NULL DEFAULT NULL,
        `bvps` DOUBLE NULL DEFAULT NULL,
        `tbvps` DOUBLE NULL DEFAULT NULL,
        `price` DOUBLE NULL DEFAULT NULL,
        `ticker` CHAR(10) NULL DEFAULT NULL,
        `dimension` CHAR(5) NULL DEFAULT NULL,
        `calendardate` DATE NULL DEFAULT NULL,
        `datekey` DATE NULL DEFAULT NULL,
        `reportperiod` DATE NULL DEFAULT NULL,
        `lastupdated` DATE NULL DEFAULT NULL,
        `security_id` INT(11) NOT NULL,
        PRIMARY KEY (`id`),
        UNIQUE INDEX `id_fundamental_UNIQUE` (`id` ASC) VISIBLE,
        UNIQUE INDEX `id_sec_date_dim` (`datekey` ASC, `security_id` ASC, `dimension` ASC) VISIBLE,
        INDEX `fk_fundamental_security1_idx` (`security_id` ASC) VISIBLE,
        CONSTRAINT `fk_fundamental_security1`
            FOREIGN KEY (`security_id`)
            REFERENCES `{}`.`security` (`id`))
        ENGINE = InnoDB
        DEFAULT CHARACTER SET = utf8;


        SET SQL_MODE=@OLD_SQL_MODE;
        SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
        SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
        
        """.format(    database_name, database_name, database_name, database_name,
        database_name, database_name, database_name, database_name, database_name,
        database_name, database_name, database_name, database_name, database_name,
        database_name, database_name, database_name, database_name, database_name, database_name )



    #print(query)

    mycursor = mydb.cursor() 
    mycursor.execute(query)

    print('ready, database generated ->  ' , database_name)

