-- MySQL dump 10.14  Distrib 5.5.46-MariaDB, for debian-linux-gnu (x86_64)
--
-- Host: localhost    Database: owldb
-- ------------------------------------------------------
-- Server version	5.5.46-MariaDB-1ubuntu0.14.04.2

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Current Database: `owldb`
--

CREATE DATABASE /*!32312 IF NOT EXISTS*/ `owldb` /*!40100 DEFAULT CHARACTER SET latin1 */;

USE `owldb`;

--
-- Table structure for table `mqtt_retain_message`
--

DROP TABLE IF EXISTS `mqtt_retain_message`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `mqtt_retain_message` (
  `topic` text NOT NULL,
  `payload` text NOT NULL,
  `qos` tinyint(2) unsigned NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `mqtt_retain_message`
--

LOCK TABLES `mqtt_retain_message` WRITE;
/*!40000 ALTER TABLE `mqtt_retain_message` DISABLE KEYS */;
INSERT INTO `mqtt_retain_message` VALUES ('topic1','this is payload',0),('topic2','this is payload',1),('topic3','this is payload',2),('topic4','this is payload',0),('topic5','this is payload',1);
/*!40000 ALTER TABLE `mqtt_retain_message` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `mqtt_subscribes`
--

DROP TABLE IF EXISTS `mqtt_subscribes`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `mqtt_subscribes` (
  `topic` text NOT NULL,
  `client_id` varchar(23) NOT NULL,
  `qos` tinyint(2) unsigned NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `mqtt_subscribes`
--

LOCK TABLES `mqtt_subscribes` WRITE;
/*!40000 ALTER TABLE `mqtt_subscribes` DISABLE KEYS */;
/*!40000 ALTER TABLE `mqtt_subscribes` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2016-01-20 18:41:48
