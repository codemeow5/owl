-- MySQL dump 10.14  Distrib 5.5.46-MariaDB, for debian-linux-gnu (x86_64)
--
-- Host: localhost    Database: owl
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
-- Table structure for table `mqtt_outgoing_messages`
--

DROP TABLE IF EXISTS `mqtt_outgoing_messages`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `mqtt_outgoing_messages` (
  `client_id` varchar(23) NOT NULL,
  `message_id` smallint(16) unsigned NOT NULL,
  `buffer` blob NOT NULL,
  `message_type` tinyint(8) unsigned NOT NULL,
  `qos` tinyint(2) unsigned NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `mqtt_outgoing_messages`
--

LOCK TABLES `mqtt_outgoing_messages` WRITE;
/*!40000 ALTER TABLE `mqtt_outgoing_messages` DISABLE KEYS */;
/*!40000 ALTER TABLE `mqtt_outgoing_messages` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `mqtt_retain_messages`
--

DROP TABLE IF EXISTS `mqtt_retain_messages`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `mqtt_retain_messages` (
  `topic` text NOT NULL,
  `payload` text NOT NULL,
  `qos` tinyint(2) unsigned NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `mqtt_retain_messages`
--

LOCK TABLES `mqtt_retain_messages` WRITE;
/*!40000 ALTER TABLE `mqtt_retain_messages` DISABLE KEYS */;
/*!40000 ALTER TABLE `mqtt_retain_messages` ENABLE KEYS */;
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

--
-- Table structure for table `mqtt_unreleased_messages`
--

DROP TABLE IF EXISTS `mqtt_unreleased_messages`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `mqtt_unreleased_messages` (
  `client_id` varchar(23) NOT NULL,
  `message_id` smallint(16) unsigned NOT NULL,
  `topic` text NOT NULL,
  `payload` text NOT NULL,
  `qos` tinyint(2) unsigned NOT NULL,
  `retain` tinyint(1) unsigned NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `mqtt_unreleased_messages`
--

LOCK TABLES `mqtt_unreleased_messages` WRITE;
/*!40000 ALTER TABLE `mqtt_unreleased_messages` DISABLE KEYS */;
/*!40000 ALTER TABLE `mqtt_unreleased_messages` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Dumping routines for database 'owl'
--
/*!50003 DROP PROCEDURE IF EXISTS `add_outgoing_message` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `add_outgoing_message`(IN `client_id_` VARCHAR(23), IN `message_id_` SMALLINT(16) UNSIGNED, IN `buffer_` BLOB, IN `message_type_` TINYINT(8) UNSIGNED, IN `qos_` TINYINT(2) UNSIGNED)
begin 
	if (select count(0) from mqtt_outgoing_messages where client_id=client_id_ and message_id=message_id_) then 
		update mqtt_outgoing_messages set buffer=buffer_, message_type=message_type_, qos=qos_ where client_id=client_id_ and message_id=message_id_; 
	else 
		insert into mqtt_outgoing_messages (client_id, message_id, buffer, message_type, qos) values (client_id_, message_id_, buffer_, message_type_, qos_); 
	end if;
end ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `add_retain_message` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `add_retain_message`(IN `topic_` TEXT, IN `payload_` TEXT, IN `qos_` TINYINT(2))
begin 
	if (select count(0) from mqtt_retain_messages where topic=topic_) then 
		update mqtt_retain_messages set payload=payload_, qos=qos_ where topic=topic_; 
	else 
		insert into mqtt_retain_messages (topic, payload, qos) values (topic_, payload_, qos_); 
	end if;
end ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `add_unreleased_message` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `add_unreleased_message`(IN `client_id_` VARCHAR(23), IN `message_id_` SMALLINT(16) UNSIGNED, IN `topic_` TEXT, IN `payload_` TEXT, IN `qos_` TINYINT(2) UNSIGNED, IN `retain_` TINYINT(1) UNSIGNED)
begin 
	if (select count(0) from mqtt_unreleased_messages where client_id=client_id_ and message_id=message_id_) then 
		update mqtt_unreleased_messages set topic=topic_, payload=payload_, qos=qos_, retain=retain_ where client_id=client_id_ and message_id=message_id_; 
	else 
		insert into mqtt_unreleased_messages (client_id, message_id, topic, payload, qos, retain) values (client_id_, message_id_, topic_, payload_, qos_, retain_); 
	end if;
end ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2016-02-23 18:47:20
