-- phpMyAdmin SQL Dump
-- version 4.4.15.7
-- http://www.phpmyadmin.net
--
-- Host: 127.0.0.1
-- Generation Time: May 15, 2019 at 09:51 AM
-- Server version: 5.6.37
-- PHP Version: 7.1.8

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Database: `customcart`
--

-- --------------------------------------------------------

--
-- Table structure for table `address_book`
--

CREATE TABLE IF NOT EXISTS `address_book` (
  `address_book_id` int(11) NOT NULL,
  `customers_id` int(11) NOT NULL DEFAULT '0',
  `entry_gender` char(1) NOT NULL,
  `entry_company` varchar(32) DEFAULT NULL,
  `entry_firstname` varchar(32) NOT NULL,
  `entry_lastname` varchar(32) NOT NULL,
  `entry_street_address` varchar(64) NOT NULL,
  `entry_suburb` varchar(32) DEFAULT NULL,
  `entry_postcode` varchar(10) NOT NULL,
  `entry_city` varchar(32) NOT NULL,
  `entry_state` varchar(32) DEFAULT NULL,
  `entry_country_id` int(11) NOT NULL DEFAULT '0',
  `entry_zone_id` int(11) NOT NULL DEFAULT '0'
) ENGINE=MyISAM AUTO_INCREMENT=7 DEFAULT CHARSET=latin1;

--
-- Dumping data for table `address_book`
--

INSERT INTO `address_book` (`address_book_id`, `customers_id`, `entry_gender`, `entry_company`, `entry_firstname`, `entry_lastname`, `entry_street_address`, `entry_suburb`, `entry_postcode`, `entry_city`, `entry_state`, `entry_country_id`, `entry_zone_id`) VALUES
(1, 1, 'm', '', 'John', 'Nguyen', '1110 E. Collins Blvd. Ste. 122', '', '75081', 'Richardson', '', 223, 57),
(3, 3, 'm', '', 'Thuy', 'Nguyen', '123 Any St.', '', '75094', 'Any City', 'TX', 1, 0),
(4, 1, 'm', '', 'John', 'Nguyen', '1700 Round Rock Dr.', NULL, '75002', 'Allen', 'TX', 1, 0);

-- --------------------------------------------------------

--
-- Table structure for table `categories`
--

CREATE TABLE IF NOT EXISTS `categories` (
  `categories_id` int(11) NOT NULL,
  `categories_image` varchar(64) DEFAULT NULL,
  `parent_id` int(11) NOT NULL DEFAULT '0',
  `sort_order` int(3) DEFAULT NULL,
  `date_added` datetime DEFAULT NULL,
  `last_modified` datetime DEFAULT NULL
) ENGINE=MyISAM AUTO_INCREMENT=122 DEFAULT CHARSET=latin1;

--
-- Dumping data for table `categories`
--

INSERT INTO `categories` (`categories_id`, `categories_image`, `parent_id`, `sort_order`, `date_added`, `last_modified`) VALUES
(48, '', 0, 50, '2008-04-14 15:27:33', '2011-10-26 12:45:07'),
(2, NULL, 0, 50, '2007-05-22 00:23:52', '2008-03-09 17:05:35'),
(3, '', 0, 50, '2007-05-22 00:23:52', '2011-07-21 10:04:35'),
(4, NULL, 0, 50, '2007-05-22 00:23:52', '2008-03-09 17:08:58'),
(5, '', 0, 50, '2007-05-22 00:23:52', '2008-07-09 10:12:53'),
(6, NULL, 0, 50, '2007-05-22 00:23:52', '2007-05-22 00:23:52'),
(7, NULL, 0, 50, '2007-05-22 00:23:52', '2007-05-22 00:23:52'),
(8, 'BSNISYNT5.04BANAPW-l.png', 7, 50, '2007-05-22 00:23:52', '2011-02-18 10:59:23'),
(9, '', 0, 50, '2007-05-22 00:23:52', '2011-10-26 12:44:49'),
(10, '', 0, 50, '2007-05-22 00:23:52', '2010-10-18 12:20:34'),
(11, '', 0, 50, '2007-05-22 00:23:52', '2011-07-21 10:04:25'),
(12, '', 0, 50, '2007-05-22 00:23:52', '2010-10-18 12:20:13'),
(13, 'labrada-lb-breakfast-applecinn-l.png', 7, 50, '2007-05-22 00:23:52', '2011-02-18 11:56:02'),
(14, 'ISSNOYGG0009CCCRBR-l.png', 7, 50, '2007-05-22 00:23:52', '2011-02-18 10:53:28'),
(15, '', 0, 50, '2007-05-22 00:23:52', '2011-07-21 11:44:12'),
(16, NULL, 0, 50, '2007-05-22 00:23:52', '2007-05-22 00:23:52'),
(17, 'dyma-elitehpgranola-7s-l.png', 7, 50, '2007-05-22 00:23:52', '2011-02-18 10:50:49'),
(101, NULL, 0, 50, '2009-04-24 11:35:33', NULL),
(19, '', 0, 50, '2007-05-22 00:23:52', '2011-07-21 10:16:02'),
(20, '', 0, 50, '2007-05-22 00:23:52', '2012-04-21 09:14:57'),
(22, NULL, 0, 50, '2007-05-22 00:23:52', '2007-05-22 00:23:52'),
(23, NULL, 0, 50, '2007-05-22 00:23:52', '2007-05-22 00:23:52'),
(24, 'ostrimjerkpepp-l.png', 7, 50, '2007-05-22 00:23:52', '2011-02-18 10:57:41'),
(25, '', 0, 50, '2007-05-22 00:23:52', '2011-10-26 12:44:57'),
(26, '', 0, 50, '2007-05-22 00:23:52', '2011-07-21 11:45:18'),
(49, NULL, 7, 50, '2008-04-14 15:28:12', '2008-04-14 15:28:12'),
(28, 'PREMCOOK0012OATMCO-l.png', 7, 50, '2007-05-22 00:23:52', '2011-02-18 10:55:51'),
(29, '', 0, 50, '2007-05-22 00:23:52', '2011-07-21 11:43:53'),
(30, NULL, 0, 50, '2007-05-22 00:23:53', '2007-05-22 00:23:53'),
(32, 'lableanbdyherstrw20ct-l.png', 7, 50, '2007-05-22 00:23:54', '2011-11-01 09:56:53'),
(33, NULL, 0, 50, '2007-05-22 00:23:54', '2007-05-22 00:23:54'),
(34, '', 0, 50, '2007-08-20 19:48:06', '2011-08-19 16:23:54'),
(35, '', 0, 50, '2007-08-20 19:48:22', NULL),
(52, NULL, 7, 50, '2008-04-14 15:28:12', '2008-04-14 15:28:12'),
(51, NULL, 7, 50, '2008-04-14 15:28:12', '2008-04-14 15:28:12'),
(50, NULL, 7, 50, '2008-04-14 15:28:12', '2008-04-14 15:28:12'),
(46, 'CSPTRTDS64PKSTRALQ-l.png', 7, 50, '2008-02-25 20:25:07', '2011-02-18 11:00:15'),
(53, NULL, 7, 50, '2008-04-14 15:28:12', '2008-04-14 15:28:12'),
(54, NULL, 7, 50, '2008-04-14 15:28:12', '2008-04-14 15:28:12'),
(55, NULL, 7, 50, '2008-04-14 15:28:12', '2008-04-14 15:28:12'),
(56, NULL, 7, 50, '2008-04-14 15:28:12', '2008-04-14 15:28:12'),
(57, NULL, 7, 50, '2008-04-14 15:28:12', '2008-04-14 15:28:12'),
(58, NULL, 7, 50, '2008-04-14 15:28:12', '2008-04-14 15:28:12'),
(59, NULL, 7, 50, '2008-04-14 15:28:12', '2008-04-14 15:28:12'),
(60, NULL, 7, 50, '2008-04-14 15:28:12', '2008-04-14 15:28:12'),
(61, NULL, 7, 50, '2008-04-14 15:28:12', '2008-04-14 15:28:12'),
(62, NULL, 7, 50, '2008-04-14 15:28:13', '2008-04-14 15:28:13'),
(63, NULL, 7, 50, '2008-04-14 15:28:13', '2008-04-14 15:28:13'),
(64, NULL, 7, 50, '2008-04-14 15:28:13', '2008-04-14 15:28:13'),
(65, NULL, 7, 50, '2008-04-14 15:28:13', '2008-04-14 15:28:13'),
(66, NULL, 7, 50, '2008-04-14 15:28:13', '2008-04-14 15:28:13'),
(67, NULL, 7, 50, '2008-04-14 15:28:13', '2008-04-14 15:28:13'),
(68, NULL, 7, 50, '2008-04-14 15:28:13', '2008-04-14 15:28:13'),
(69, NULL, 7, 50, '2008-04-14 15:28:13', '2008-04-14 15:28:13'),
(70, NULL, 7, 50, '2008-04-14 15:28:13', '2008-04-14 15:28:13'),
(71, NULL, 7, 50, '2008-04-14 15:28:13', '2008-04-14 15:28:13'),
(72, NULL, 7, 50, '2008-04-14 15:28:13', '2008-04-14 15:28:13'),
(73, NULL, 7, 50, '2008-04-14 15:28:13', '2008-04-14 15:28:13'),
(74, NULL, 7, 50, '2008-04-14 15:28:14', '2008-04-14 15:28:14'),
(75, NULL, 7, 50, '2008-04-14 15:28:14', '2008-04-14 15:28:14'),
(76, NULL, 7, 50, '2008-04-14 15:28:14', '2008-04-14 15:28:14'),
(77, NULL, 7, 50, '2008-04-14 15:28:14', '2008-04-14 15:28:14'),
(78, NULL, 7, 50, '2008-04-14 15:28:14', '2008-04-14 15:28:14'),
(79, NULL, 7, 50, '2008-04-14 15:28:14', '2008-04-14 15:28:14'),
(80, NULL, 7, 50, '2008-04-14 15:28:14', '2008-04-14 15:28:14'),
(81, NULL, 7, 50, '2008-04-14 15:28:14', '2008-04-14 15:28:14'),
(82, NULL, 7, 50, '2008-04-14 15:28:14', '2008-04-14 15:28:14'),
(83, NULL, 7, 50, '2008-04-14 15:28:14', '2008-04-14 15:28:14'),
(84, NULL, 7, 50, '2008-04-14 15:28:14', '2008-04-14 15:28:14'),
(85, NULL, 7, 50, '2008-04-14 15:28:15', '2008-04-14 15:28:15'),
(100, NULL, 0, 50, '2009-04-24 11:35:02', NULL),
(87, NULL, 7, 50, '2008-04-14 15:28:15', '2008-04-14 15:28:15'),
(88, NULL, 7, 50, '2008-04-14 15:28:15', '2008-04-14 15:28:15'),
(89, NULL, 7, 50, '2008-04-14 15:28:16', '2008-04-14 15:28:16'),
(90, NULL, 7, 50, '2008-04-14 15:28:16', '2008-04-14 15:28:16'),
(91, NULL, 7, 50, '2008-04-14 15:28:16', '2008-04-14 15:28:16'),
(92, NULL, 7, 50, '2008-04-14 15:28:16', '2008-04-14 15:28:16'),
(93, NULL, 7, 50, '2008-04-14 15:28:17', '2008-04-14 15:28:17'),
(94, NULL, 7, 50, '2008-04-14 15:28:17', '2008-04-14 15:28:17'),
(95, NULL, 7, 50, '2008-04-14 15:28:17', '2008-04-14 15:28:17'),
(96, NULL, 7, 50, '2008-04-14 15:28:17', '2008-04-14 15:28:17'),
(97, NULL, 7, 50, '2008-04-14 15:28:17', '2008-04-14 15:28:17'),
(98, NULL, 7, 50, '2008-04-14 15:28:17', '2008-04-14 15:28:17'),
(99, 'GRANOATS0012PEANPK-l.png', 7, 50, '2008-04-14 20:04:27', '2011-02-18 11:07:41'),
(102, '', 0, 50, '2009-04-24 11:35:37', '2011-07-21 11:45:08'),
(103, '', 0, 50, '2009-04-24 11:50:42', '2011-07-21 11:44:51'),
(104, NULL, 0, 50, '2010-04-20 08:01:46', NULL),
(107, '', 0, 50, '2010-10-13 13:19:54', '2011-07-21 10:04:47'),
(110, NULL, 0, 50, '2010-10-14 10:56:12', NULL),
(114, '', 0, 50, '2010-11-18 18:14:41', '2011-07-21 11:44:43'),
(115, 'IDS-newwhey-acai-l.png', 7, 50, '2010-12-16 15:42:13', '2011-02-18 11:01:01'),
(116, '', 0, 50, '2010-12-23 14:38:23', '2011-07-21 11:45:00'),
(117, '', 0, 50, '2011-04-27 17:38:20', '2011-07-21 11:44:34'),
(118, '', 0, 50, '2011-07-08 11:08:22', '2011-09-08 16:31:57');

-- --------------------------------------------------------

--
-- Table structure for table `categories_description`
--

CREATE TABLE IF NOT EXISTS `categories_description` (
  `categories_id` int(11) NOT NULL DEFAULT '0',
  `language_id` int(11) NOT NULL DEFAULT '1',
  `categories_name` varchar(32) NOT NULL
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

--
-- Dumping data for table `categories_description`
--

INSERT INTO `categories_description` (`categories_id`, `language_id`, `categories_name`) VALUES
(48, 1, 'Mood Enhancer'),
(2, 1, 'Muscle Enhancers'),
(3, 1, 'Pre Workout'),
(4, 1, 'Testosterone Booster'),
(5, 1, 'Weight Loss'),
(6, 1, 'Liver Support'),
(7, 1, 'Protein'),
(8, 1, 'Protein Powder'),
(9, 1, 'Energy Booster'),
(10, 1, 'Creatine'),
(11, 1, 'Post Workout'),
(12, 1, 'Accessories'),
(13, 1, 'Meal Replacement'),
(14, 1, 'Protein Bars'),
(15, 1, 'Vascular Dilators'),
(16, 1, 'Weight Gainer'),
(17, 1, 'Oatmeal'),
(103, 1, 'Cholesterol Reducers'),
(19, 1, 'Detoxification'),
(20, 1, 'Vitamins and Minerals'),
(22, 1, 'Joint Support'),
(23, 1, 'Glutamine'),
(24, 1, 'Protein Jerky'),
(25, 1, 'Immune Booster'),
(26, 1, 'Tanning Products'),
(102, 1, 'Sleep Aids'),
(28, 1, 'Protein Cookies'),
(29, 1, 'Water Loss'),
(30, 1, 'Amino Acids'),
(32, 1, 'Protein For Women'),
(33, 1, 'Recovery Formula'),
(34, 1, 'Beneficial Oils'),
(35, 1, 'Antioxidants'),
(99, 1, 'Protein Snacks'),
(46, 1, 'Protein RTDs'),
(117, 1, 'Digestive Enzymes'),
(107, 1, 'Intra Workout'),
(114, 1, 'Cellulite Cream'),
(110, 1, 'Nitric Oxide'),
(115, 1, 'Protein Shots'),
(116, 1, 'Natural'),
(118, 1, 'Adrenal Support');

-- --------------------------------------------------------

--
-- Table structure for table `countries`
--

CREATE TABLE IF NOT EXISTS `countries` (
  `countries_id` int(11) NOT NULL,
  `countries_name` varchar(64) NOT NULL,
  `countries_iso_code_2` char(2) NOT NULL,
  `countries_iso_code_3` char(3) NOT NULL,
  `address_format_id` int(11) NOT NULL DEFAULT '0'
) ENGINE=MyISAM AUTO_INCREMENT=224 DEFAULT CHARSET=latin1;

--
-- Dumping data for table `countries`
--

INSERT INTO `countries` (`countries_id`, `countries_name`, `countries_iso_code_2`, `countries_iso_code_3`, `address_format_id`) VALUES
(223, 'United States', 'US', 'USA', 2);

-- --------------------------------------------------------

--
-- Table structure for table `currencies`
--

CREATE TABLE IF NOT EXISTS `currencies` (
  `currencies_id` int(11) NOT NULL,
  `title` varchar(32) NOT NULL,
  `code` char(3) NOT NULL,
  `symbol_left` varchar(12) DEFAULT NULL,
  `symbol_right` varchar(12) DEFAULT NULL,
  `decimal_point` char(1) DEFAULT NULL,
  `thousands_point` char(1) DEFAULT NULL,
  `decimal_places` char(1) DEFAULT NULL,
  `value` float(13,8) DEFAULT NULL,
  `last_updated` datetime DEFAULT NULL
) ENGINE=MyISAM AUTO_INCREMENT=3 DEFAULT CHARSET=latin1;

--
-- Dumping data for table `currencies`
--

INSERT INTO `currencies` (`currencies_id`, `title`, `code`, `symbol_left`, `symbol_right`, `decimal_point`, `thousands_point`, `decimal_places`, `value`, `last_updated`) VALUES
(1, 'US Dollar', 'USD', '$', '', '.', ',', '2', 1.00000000, '2005-07-07 10:38:23');

-- --------------------------------------------------------

--
-- Table structure for table `customers`
--

CREATE TABLE IF NOT EXISTS `customers` (
  `customers_id` int(11) NOT NULL,
  `customers_gender` char(1) NOT NULL,
  `customers_firstname` varchar(32) NOT NULL,
  `customers_lastname` varchar(32) NOT NULL,
  `customers_dob` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `customers_email_address` varchar(96) NOT NULL,
  `customers_default_address_id` int(11) NOT NULL DEFAULT '0',
  `customers_telephone` varchar(32) NOT NULL,
  `customers_fax` varchar(32) DEFAULT NULL,
  `customers_password` varchar(40) NOT NULL,
  `customers_newsletter` char(1) DEFAULT NULL
) ENGINE=MyISAM AUTO_INCREMENT=6 DEFAULT CHARSET=latin1;

--
-- Dumping data for table `customers`
--

INSERT INTO `customers` (`customers_id`, `customers_gender`, `customers_firstname`, `customers_lastname`, `customers_dob`, `customers_email_address`, `customers_default_address_id`, `customers_telephone`, `customers_fax`, `customers_password`, `customers_newsletter`) VALUES
(1, 'm', 'John', 'Nguyen', '1975-09-10 00:00:00', 'john@lsfulfillment.com', 1, '972-644-1011', '', '2e7687bfd53846bcd73be75eef88e07a:74', '0'),
(3, 'm', 'Thuy', 'Nguyen', '1970-05-21 00:00:00', 'tnguyen@fiveseventeensolutions.com', 3, '1234567896', '', 'aaa784cb1d3e1936b0644f1e5acdce60:0c', '');

-- --------------------------------------------------------

--
-- Table structure for table `customers_info`
--

CREATE TABLE IF NOT EXISTS `customers_info` (
  `customers_info_id` int(11) NOT NULL DEFAULT '0',
  `customers_info_date_of_last_logon` datetime DEFAULT NULL,
  `customers_info_number_of_logons` int(5) DEFAULT NULL,
  `customers_info_date_account_created` datetime DEFAULT NULL,
  `customers_info_date_account_last_modified` datetime DEFAULT NULL,
  `global_product_notifications` int(1) DEFAULT '0'
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

--
-- Dumping data for table `customers_info`
--

INSERT INTO `customers_info` (`customers_info_id`, `customers_info_date_of_last_logon`, `customers_info_number_of_logons`, `customers_info_date_account_created`, `customers_info_date_account_last_modified`, `global_product_notifications`) VALUES
(1, '2008-07-27 21:34:49', 21, '2007-05-18 11:37:08', '2007-08-22 14:29:57', 0),
(3, '2007-06-15 00:27:14', 5, '2007-05-22 21:03:01', NULL, 0);

-- --------------------------------------------------------

--
-- Table structure for table `languages`
--

CREATE TABLE IF NOT EXISTS `languages` (
  `languages_id` int(11) NOT NULL,
  `name` varchar(32) NOT NULL,
  `code` char(2) NOT NULL,
  `image` varchar(64) DEFAULT NULL,
  `directory` varchar(32) DEFAULT NULL,
  `sort_order` int(3) DEFAULT NULL
) ENGINE=MyISAM AUTO_INCREMENT=2 DEFAULT CHARSET=latin1;

--
-- Dumping data for table `languages`
--

INSERT INTO `languages` (`languages_id`, `name`, `code`, `image`, `directory`, `sort_order`) VALUES
(1, 'English', 'en', 'icon.gif', 'english', 1);

-- --------------------------------------------------------

--
-- Table structure for table `orders`
--

CREATE TABLE IF NOT EXISTS `orders` (
  `orders_id` int(11) NOT NULL,
  `client_customers_id` int(11) NOT NULL DEFAULT '0',
  `customers_telephone` varchar(255) CHARACTER SET latin1 DEFAULT NULL,
  `customers_email` varchar(255) CHARACTER SET latin1 DEFAULT NULL,
  `shipping_method` varchar(255) CHARACTER SET latin1 DEFAULT NULL,
  `tracking_number` varchar(255) CHARACTER SET latin1 DEFAULT NULL,
  `ref_number` varchar(255) NOT NULL,
  `order_number` varchar(255) DEFAULT NULL,
  `delivery_name` varchar(64) CHARACTER SET latin1 NOT NULL,
  `delivery_state` varchar(255) CHARACTER SET latin1 DEFAULT NULL,
  `delivery_country` varchar(255) CHARACTER SET latin1 DEFAULT NULL,
  `delivery_company` varchar(255) CHARACTER SET latin1 DEFAULT NULL,
  `delivery_suburb` varchar(255) CHARACTER SET latin1 DEFAULT NULL,
  `delivery_city` varchar(255) CHARACTER SET latin1 DEFAULT NULL,
  `delivery_address2` varchar(255) CHARACTER SET latin1 DEFAULT NULL,
  `delivery_address1` varchar(255) CHARACTER SET latin1 NOT NULL,
  `delivery_postcode` varchar(10) CHARACTER SET latin1 NOT NULL,
  `billing_suburb` varchar(255) CHARACTER SET latin1 DEFAULT NULL,
  `billing_company` varchar(255) CHARACTER SET latin1 DEFAULT NULL,
  `billing_city` varchar(255) CHARACTER SET latin1 DEFAULT NULL,
  `billing_country` varchar(255) CHARACTER SET latin1 DEFAULT NULL,
  `billing_state` varchar(255) CHARACTER SET latin1 DEFAULT NULL,
  `billing_address2` varchar(255) CHARACTER SET latin1 DEFAULT NULL,
  `billing_name` varchar(64) CHARACTER SET latin1 NOT NULL,
  `billing_address1` varchar(64) CHARACTER SET latin1 NOT NULL,
  `billing_postcode` varchar(10) CHARACTER SET latin1 NOT NULL,
  `payment_method` varchar(32) CHARACTER SET latin1 NOT NULL,
  `cc_type` varchar(20) CHARACTER SET latin1 DEFAULT NULL,
  `cc_owner` varchar(64) CHARACTER SET latin1 DEFAULT NULL,
  `cc_number` varchar(32) CHARACTER SET latin1 DEFAULT NULL,
  `cc_expires` varchar(4) CHARACTER SET latin1 DEFAULT NULL,
  `last_modified` datetime DEFAULT NULL,
  `date_purchased` datetime DEFAULT NULL,
  `orders_status` varchar(30) NOT NULL DEFAULT '0',
  `orders_date_finished` datetime DEFAULT NULL,
  `currency` char(3) CHARACTER SET latin1 DEFAULT NULL,
  `currency_value` decimal(14,6) DEFAULT NULL,
  `client_navision_id` varchar(64) CHARACTER SET latin1 DEFAULT NULL,
  `client_orders_id` varchar(64) CHARACTER SET latin1 DEFAULT NULL,
  `order_total` decimal(15,4) DEFAULT NULL
) ENGINE=MyISAM AUTO_INCREMENT=9 DEFAULT CHARSET=utf8;

--
-- Dumping data for table `orders`
--

INSERT INTO `orders` (`orders_id`, `client_customers_id`, `customers_telephone`, `customers_email`, `shipping_method`, `tracking_number`, `ref_number`, `order_number`, `delivery_name`, `delivery_state`, `delivery_country`, `delivery_company`, `delivery_suburb`, `delivery_city`, `delivery_address2`, `delivery_address1`, `delivery_postcode`, `billing_suburb`, `billing_company`, `billing_city`, `billing_country`, `billing_state`, `billing_address2`, `billing_name`, `billing_address1`, `billing_postcode`, `payment_method`, `cc_type`, `cc_owner`, `cc_number`, `cc_expires`, `last_modified`, `date_purchased`, `orders_status`, `orders_date_finished`, `currency`, `currency_value`, `client_navision_id`, `client_orders_id`, `order_total`) VALUES
(1, 0, '972-644-1011', NULL, NULL, NULL, '', NULL, 'John Nguyen', 'TX', 'United States', '', '', 'Richardson', NULL, '', '75081', '', '', 'Richardson', 'United States', 'TX', NULL, 'John Nguyen', '', '75081', 'Cash on Delivery', '', '', '', '', NULL, '2007-05-18 11:46:26', '1', NULL, 'USD', 1.000000, NULL, NULL, NULL),
(2, 0, 'test', NULL, NULL, NULL, '', NULL, 'test test', '1111111111', 'United States', '', 'test', 'test', NULL, '', 'test', 'test', '', 'test', 'United States', '1111111111', NULL, 'test test', '', 'test', 'Cash on Delivery', '', '', '', '', NULL, '2007-05-21 08:26:05', '1', NULL, 'USD', 1.000000, NULL, NULL, NULL),
(3, 0, 'test', NULL, NULL, NULL, '', NULL, 'test test', '1111111111', 'United States', '', 'test', 'test', NULL, '', 'test', 'test', '', 'test', 'United States', '1111111111', NULL, 'test test', '', 'test', 'Cash on Delivery', '', '', '', '', NULL, '2007-05-22 08:42:38', '1', NULL, 'USD', 1.000000, NULL, NULL, NULL),
(4, 0, '972-644-1011', NULL, NULL, NULL, '', NULL, 'John Nguyen', 'TX', 'United States', '', '', 'Richardson', NULL, '', '75081', '', '', 'Richardson', 'United States', 'TX', NULL, 'John Nguyen', '', '75081', 'Authorize.net', '', '', '', '', NULL, '2007-06-14 20:30:36', '1', NULL, 'USD', 1.000000, NULL, NULL, NULL),
(5, 0, '512-670-0047', NULL, NULL, NULL, '', NULL, 'fernando barbosa', 'texas', 'United States', 'dalech,llc', '', 'pflugerville', NULL, '', '78660', '', 'dalech,llc', 'pflugerville', 'United States', 'texas', NULL, 'fernando barbosa', '', '78660', 'Credit Card', '', '', '', '', NULL, '2007-06-15 07:22:23', '1', NULL, 'USD', 1.000000, NULL, NULL, NULL),
(6, 0, '972-644-1011', NULL, NULL, NULL, '', NULL, 'John Nguyen', 'TX', 'United States', '', '', 'Richardson', NULL, '', '75081', '', '', 'Richardson', 'United States', 'TX', NULL, 'John Nguyen', '', '75081', 'Credit Card', '', '', '', '', NULL, '2007-06-15 19:19:07', '1', NULL, 'USD', 1.000000, NULL, NULL, NULL),
(7, 0, '512-670-0047', NULL, NULL, NULL, '', NULL, 'FERNANDO BARBOSA', 'TEXAS', 'United States', 'DALECH LLC', '', 'PFLUGERVILLE', NULL, '', '78660', '', 'DALECH LLC', 'PFLUGERVILLE', 'United States', 'TEXAS', NULL, 'FERNANDO BARBOSA', '', '78660', 'Credit Card', '', '', '', '', NULL, '2007-06-27 15:57:37', '1', NULL, 'USD', 1.000000, NULL, NULL, NULL),
(8, 0, '972-644-1011', NULL, NULL, NULL, '', NULL, 'John Nguyen', 'Texas', 'United States', '', '', 'Richardson', NULL, '', '75081', '', '', 'Richardson', 'United States', 'Texas', NULL, 'John Nguyen', '', '75081', 'Cash on Delivery', '', '', '', '', NULL, '2008-07-27 21:32:18', '1', NULL, 'USD', 1.000000, NULL, NULL, NULL);

-- --------------------------------------------------------

--
-- Table structure for table `orders_products`
--

CREATE TABLE IF NOT EXISTS `orders_products` (
  `orders_products_id` int(11) NOT NULL,
  `orders_id` int(11) NOT NULL DEFAULT '0',
  `products_name` varchar(64) NOT NULL,
  `products_price` decimal(15,4) NOT NULL DEFAULT '0.0000',
  `final_price` decimal(15,4) NOT NULL DEFAULT '0.0000',
  `products_tax` decimal(7,4) NOT NULL DEFAULT '0.0000',
  `products_quantity` int(2) NOT NULL DEFAULT '0',
  `products_lsfs_item_id` varchar(100) DEFAULT NULL,
  `client_orders_id` int(11) DEFAULT NULL,
  `products_upc_code` varchar(255) NOT NULL
) ENGINE=MyISAM AUTO_INCREMENT=11 DEFAULT CHARSET=latin1;

--
-- Dumping data for table `orders_products`
--

INSERT INTO `orders_products` (`orders_products_id`, `orders_id`, `products_name`, `products_price`, `final_price`, `products_tax`, `products_quantity`, `products_lsfs_item_id`, `client_orders_id`, `products_upc_code`) VALUES
(1, 1, 'ALRI: Regeneration X Watermelon Margarita', 0.0000, 0.0000, 0.0000, 1, NULL, NULL, ''),
(2, 1, 'ALRI: Badd Ass Mass 60ct', 0.0000, 0.0000, 0.0000, 1, NULL, NULL, ''),
(3, 2, 'Freedom: The Liquid Stuff Grape 16oz', 0.0000, 0.0000, 0.0000, 1, NULL, NULL, ''),
(4, 3, 'Betancourt: Glutamine High C Fruit Punch 60sr', 0.0000, 0.0000, 0.0000, 1, NULL, NULL, ''),
(5, 3, 'Zoller: Zantrex-3 84 ct', 0.0000, 0.0000, 0.0000, 1, NULL, NULL, ''),
(6, 4, 'Zoller: Zantrex-3 84 ct', 32.9890, 32.9890, 0.0000, 1, NULL, NULL, ''),
(7, 5, 'Labrada: Lean Body Gold Bar 12ct Caramel Cook', 26.5665, 26.5665, 8.2500, 1, NULL, NULL, ''),
(8, 6, 'World Wide: Pure Pro Bars 12ct Smore''s', 24.0300, 24.0300, 8.2500, 1, NULL, NULL, ''),
(9, 7, 'Labrada: Lean Body Gold Bar 12ct Caramel Cook', 26.5665, 26.5665, 8.2500, 1, NULL, NULL, ''),
(10, 8, 'Labrada: Lean Body Gold Bar Caramel Cookie Twist', 26.5665, 26.5665, 0.0000, 1, NULL, NULL, '');

-- --------------------------------------------------------

--
-- Table structure for table `orders_total`
--

CREATE TABLE IF NOT EXISTS `orders_total` (
  `orders_total_id` int(10) unsigned NOT NULL,
  `orders_id` int(11) NOT NULL DEFAULT '0',
  `title` varchar(255) NOT NULL,
  `text` varchar(255) NOT NULL,
  `value` decimal(15,4) NOT NULL DEFAULT '0.0000',
  `class` varchar(32) NOT NULL,
  `sort_order` int(11) NOT NULL DEFAULT '0'
) ENGINE=MyISAM AUTO_INCREMENT=27 DEFAULT CHARSET=latin1;

--
-- Dumping data for table `orders_total`
--

INSERT INTO `orders_total` (`orders_total_id`, `orders_id`, `title`, `text`, `value`, `class`, `sort_order`) VALUES
(1, 1, 'Sub-Total:', '$0.00', 0.0000, 'ot_subtotal', 1),
(2, 1, 'Flat Rate (Best Way):', '$5.00', 5.0000, 'ot_shipping', 2),
(3, 1, 'Total:', '<b>$5.00</b>', 5.0000, 'ot_total', 4),
(4, 2, 'Sub-Total:', '$0.00', 0.0000, 'ot_subtotal', 1),
(5, 2, 'Flat Rate (Best Way):', '$5.00', 5.0000, 'ot_shipping', 2),
(6, 2, 'Total:', '<b>$5.00</b>', 5.0000, 'ot_total', 4),
(7, 3, 'Sub-Total:', '$0.00', 0.0000, 'ot_subtotal', 1),
(8, 3, 'Flat Rate (Best Way):', '$5.00', 5.0000, 'ot_shipping', 2),
(9, 3, 'Total:', '<b>$5.00</b>', 5.0000, 'ot_total', 4),
(10, 4, 'Sub-Total:', '$32.99', 32.9900, 'ot_subtotal', 1),
(11, 4, 'Flat Rate (Best Way):', '$5.00', 5.0000, 'ot_shipping', 2),
(12, 4, 'Total:', '<b>$37.99</b>', 37.9900, 'ot_total', 4),
(13, 5, 'Sub-Total:', '$26.57', 26.5700, 'ot_subtotal', 1),
(14, 5, 'Zone Rates (Shipping to US : 3 lb(s)):', '$8.50', 8.5000, 'ot_shipping', 2),
(15, 5, 'Texas Sales Tax:', '$2.19', 2.1920, 'ot_tax', 3),
(16, 5, 'Total:', '<b>$37.26</b>', 37.2620, 'ot_total', 4),
(17, 6, 'Sub-Total:', '$24.03', 24.0300, 'ot_subtotal', 1),
(18, 6, 'United Parcel Service (XML) (1 pkg x 3 lbs total) (UPS Ground):', '$8.20', 8.2000, 'ot_shipping', 2),
(19, 6, 'Texas Sales Tax:', '$1.98', 1.9825, 'ot_tax', 3),
(20, 6, 'Total:', '<b>$34.21</b>', 34.2125, 'ot_total', 4),
(21, 7, 'Sub-Total:', '$26.57', 26.5700, 'ot_subtotal', 1),
(22, 7, 'United Parcel Service (XML) (1 pkg x 5 lbs total) (UPS Ground):', '$8.88', 8.8800, 'ot_shipping', 2),
(23, 7, 'Texas Sales Tax:', '$2.19', 2.1920, 'ot_tax', 3),
(24, 7, 'Total:', '<b>$37.64</b>', 37.6420, 'ot_total', 4),
(25, 8, 'Sub-Total:', '$26.57', 26.5700, 'ot_subtotal', 1),
(26, 8, 'Total:', '<b>$26.57</b>', 26.5700, 'ot_total', 4);

-- --------------------------------------------------------

--
-- Table structure for table `products`
--

CREATE TABLE IF NOT EXISTS `products` (
  `products_id` int(11) NOT NULL,
  `products_quantity` int(7) NOT NULL DEFAULT '0',
  `products_quantity_fl` int(5) NOT NULL DEFAULT '0',
  `products_quantity_ga` int(5) NOT NULL DEFAULT '0',
  `products_quantity_ct` int(5) NOT NULL DEFAULT '0',
  `products_quantity_tx` int(5) NOT NULL DEFAULT '0',
  `products_quantity_in` int(5) NOT NULL DEFAULT '0',
  `products_model` varchar(32) DEFAULT NULL,
  `products_image` varchar(64) DEFAULT NULL,
  `products_price` decimal(15,4) NOT NULL DEFAULT '0.0000',
  `products_date_added` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `products_last_modified` datetime DEFAULT NULL,
  `products_date_available` datetime DEFAULT NULL,
  `products_weight` decimal(5,2) NOT NULL DEFAULT '0.00',
  `products_status` tinyint(1) NOT NULL DEFAULT '0',
  `products_tax_class_id` int(11) NOT NULL DEFAULT '0',
  `manufacturers_id` int(11) DEFAULT NULL,
  `products_ordered` int(11) NOT NULL DEFAULT '0',
  `products_upc_code` varchar(30) DEFAULT NULL,
  `products_image_large` varchar(64) NOT NULL,
  `products_wholesale_price` decimal(15,4) NOT NULL,
  `products_msrp` float DEFAULT NULL,
  `products_cost` float DEFAULT NULL,
  `products_lsfs` varchar(45) NOT NULL DEFAULT '0',
  `lsfs_cost` float DEFAULT NULL,
  `lsfs_item_id` varchar(50) NOT NULL,
  `products_length` float DEFAULT NULL,
  `products_width` float DEFAULT NULL,
  `products_height` float DEFAULT NULL,
  `products_mpn` varchar(30) DEFAULT NULL,
  `products_discount_group` varchar(30) NOT NULL,
  `product_dimensions` varchar(64) DEFAULT NULL
) ENGINE=MyISAM AUTO_INCREMENT=4624 DEFAULT CHARSET=latin1;

--
-- Dumping data for table `products`
--

INSERT INTO `products` (`products_id`, `products_quantity`, `products_quantity_fl`, `products_quantity_ga`, `products_quantity_ct`, `products_quantity_tx`, `products_quantity_in`, `products_model`, `products_image`, `products_price`, `products_date_added`, `products_last_modified`, `products_date_available`, `products_weight`, `products_status`, `products_tax_class_id`, `manufacturers_id`, `products_ordered`, `products_upc_code`, `products_image_large`, `products_wholesale_price`, `products_msrp`, `products_cost`, `products_lsfs`, `lsfs_cost`, `lsfs_item_id`, `products_length`, `products_width`, `products_height`, `products_mpn`, `products_discount_group`, `product_dimensions`) VALUES
(9, 104, 9, 11, 22, 54, 8, 'BSNIDESS1.39CHCOPW', 'BSNIDESS1.39CHCOPW-100.png', 0.0000, '2007-05-22 00:23:52', '2013-02-02 09:00:02', NULL, 1.75, 1, 1, 4, 0, '834266006021', 'BSNIDESS1.39CHCOPW-200.png', 21.7700, 34.99, 0, '1', 13.686, 'BSNIDESS1.39CHCOPW', 5, 5, 7, '602', 'BSN', NULL),
(10, 253, 51, 15, 34, 148, 5, 'BSNIDESS1.39VANNPW', 'BSNIDESS1.39VANNPW-100.png', 0.0000, '2007-05-22 00:23:52', '2013-02-21 16:20:02', NULL, 1.70, 1, 1, 4, 0, '834266008070', 'BSNIDESS1.39VANNPW-200.png', 21.7700, 33.49, 0, '1', 13.72, 'BSNIDESS1.39VANNPW', 5, 5, 7, '807', 'BSN', NULL),
(16, 24, 14, 6, 3, 4, -3, 'CHMPNUTR0060CHOCPK', 'chpultmtchoc60ct-s.png', 0.0000, '2007-05-22 00:23:52', '2013-02-02 09:00:02', NULL, 12.05, 1, 1, 5, 0, '027692111203', 'chpultmtchoc60ct-l.png', 150.2900, 231.22, 0, '1', 97.696, 'CHMPNUTR0060CHOCPK', 16.5, 11.25, 8, '027692111203', 'CHAMPION', NULL),
(17, -2, -3, 8, -3, -1, -3, 'CHMPNULC0060CHOCPK', 'CHMPNULC0060CHOCPK-s.png', 0.0000, '2007-05-22 00:23:52', '2013-02-02 09:00:02', NULL, 9.25, 1, 1, 5, 0, '027692132208', 'CHMPNULC0060CHOCPK-l.png', 123.3000, 189.7, 0, '1', 80.143, 'CHMPNULC0060CHOCPK', 16.5, 11.25, 8, '', 'CHAMPION', NULL),
(18, 4, -3, 3, -3, 5, 2, 'CHMPNULC0060VANIPK', 'chpultmtlcvan60ct-s.png', 0.0000, '2007-05-22 00:23:52', '2013-01-04 23:40:03', NULL, 9.00, 1, 1, 5, 0, '027692132307', 'chpultmtlcvan60ct-l.png', 123.3000, 189.7, 0, '1', 80.146, 'CHMPNULC0060VANIPK', 16.5, 11.25, 8, '027692132307', 'CHAMPION', NULL),
(19, 6, 5, -1, -3, 6, -1, 'CHMPNUTR0060STRAPK', 'chpultrametstrw60ct-s.png', 0.0000, '2007-05-22 00:23:52', '2013-01-28 00:00:03', NULL, 12.00, 1, 1, 5, 0, '027692118400', 'chpultrametstrw60ct-l.png', 150.2900, 231.22, 0, '1', 97.693, 'CHMPNUTR0060STRAPK', 16.5, 11.25, 8, '', 'CHAMPION', NULL),
(20, 6, 4, 1, -3, 5, -1, 'CHMPNUTR0060VANLPK', 'chpultrmtvan60ct-s.png', 0.0000, '2007-05-22 00:23:52', '2013-01-28 00:00:03', NULL, 12.00, 1, 1, 5, 0, '027692130600', 'chpultrmtvan60ct-l.png', 150.2900, 231.22, 0, '1', 97.69, 'CHMPNUTR0060VANLPK', 16.5, 11.25, 8, '027692111005', 'CHAMPION', NULL),
(22, 45, 26, 2, -2, 19, 0, 'CLIFCLBR0012BLUEBR', 'clfbarbberrycrsp12ct-s.png', 0.0000, '2007-05-22 00:23:52', '2012-10-12 07:41:21', NULL, 2.05, 1, 1, 6, 0, '722252302601', 'clfbarbberrycrsp12ct-l.png', 14.5000, 20.28, 0, '1', 9.72, 'CLIFCLBR0012BLUEBR', 11, 5.5, 2.25, '161007', 'CLIF', NULL),
(23, 69, 6, -3, 6, 34, 26, 'CLIFBUIL0012PEANBR', 'CLIFBUIL0012PEANBR-s.png', 0.0000, '2007-05-22 00:23:52', '2012-09-05 21:20:03', NULL, 2.05, 1, 1, 6, 0, '722252600417', 'CLIFBUIL0012PEANBR-l.png', 20.2000, 28.68, 0, '1', 13.2, 'CLIFBUIL0012PEANBR', 9, 6, 2.25, '160041', 'CLIF', NULL),
(24, 228, 83, 0, 48, 84, 13, 'CLIFCLBR0012CCHPBR', 'clfbarchocchip12ct-s.png', 0.0000, '2007-05-22 00:23:52', '2013-01-28 00:00:03', NULL, 2.00, 1, 1, 6, 0, '722252300904', 'clfbarchocchip12ct-l.png', 14.5000, 20.28, 0, '1', 9.721, 'CLIFCLBR0012CCHPBR', 11, 5.5, 2.25, '160004', 'CLIF', NULL),
(25, 39, -3, 3, 12, 21, 6, 'CLIFLUNA0015CARABR', 'clflunaoatmeal12ct-s.png', 0.0000, '2007-05-22 00:23:52', '2012-08-13 16:00:03', NULL, 1.90, 1, 1, 6, 0, '722252200631', 'clflunaoatmeal12ct-l.png', 19.2000, 26.85, 0, '1', 12.15, 'CLIFLUNA0015CARABR', 5.25, 10.25, 2.25, '210063', 'CLIF', NULL),
(37, 15, 0, 1, -3, 6, 11, 'CSPTCMAX01.5CITRPW', 'CSPTCMAX01.5CITRPW-s.png', 0.0000, '2007-05-22 00:23:52', '2013-02-02 09:00:03', NULL, 1.80, 1, 1, 8, 0, '660726103104', 'CSPTCMAX01.5CITRPW-l.png', 13.1700, 21.95, 0, '1', 8.399, 'CSPTCMAX01.5CITRPW', 5, 5, 6.25, '60726-10310', 'CYTOSPORT', NULL),
(39, 15, 5, 1, -3, 11, 1, 'CSPTCMAX01.5ORGEPW', 'CSPTCMAX01.5ORGEPW1-s.png', 0.0000, '2007-05-22 00:23:52', '2013-02-02 09:00:03', NULL, 1.80, 1, 1, 8, 0, '660726104705', 'CSPTCMAX01.5ORGEPW1-l.png', 13.1700, 21.95, 0, '1', 8.158, 'CSPTCMAX01.5ORGEPW', 5, 5, 6.25, '60726-10470', 'CYTOSPORT', NULL),
(41, 3, 6, -1, 0, 1, -3, 'CSPTEVOP2.25VANNPW', 'CSPTEVOP2.25VANNPW-100.png', 0.0000, '2007-05-22 00:23:52', '2012-11-19 23:00:03', NULL, 2.85, 1, 1, 8, 0, '660726780107', 'CSPTEVOP2.25VANNPW-200.png', 42.5700, 65.49, 0, '1', 27.14, 'CSPTEVOP2.25VANNPW', 5.5, 5.5, 9.5, '60726-78010', 'CYTOSPORT', NULL),
(42, 47, 12, 2, -1, 22, 12, 'CSPTFAST2.04PUNCPW', 'cysfasttwtchlem2lb-s.png', 0.0000, '2007-05-22 00:23:52', '2012-11-03 20:20:03', NULL, 2.45, 1, 1, 8, 0, '660726800010', 'cysfasttwtchlem2lb-l.png', 34.3400, 54.95, 0, '1', 21.88, 'CSPTFAST2.04PUNCPW', 5.5, 5.5, 8, '60726-80001', 'CYTOSPORT', NULL),
(43, 6, 1, 3, 1, 1, 0, 'CSPTFAST2.04GRAPPW', 'cysfasttwtchgrp2lb-s.png', 0.0000, '2007-05-22 00:23:52', '2012-08-10 17:00:03', NULL, 2.40, 1, 1, 8, 0, '660726800027', 'cysfasttwtchgrp2lb-l.png', 34.3400, 54.95, 0, '1', 21.88, 'CSPTFAST2.04GRAPPW', 5.5, 5.5, 8, '60726-80002', 'CYTOSPORT', NULL),
(44, 0, 0, 0, -3, 0, 3, 'CSPTFAST2.01LEMOPW', 'cysfasttwtchlem2lb-s.png', 0.0000, '2007-05-22 00:23:52', '2012-11-03 20:20:03', NULL, 2.40, 1, 1, 8, 0, '660726800034', 'cysfasttwtchlem2lb-l.png', 34.3400, 54.95, 0, '1', 21.88, 'CSPTFAST2.01LEMOPW', 5.5, 5.5, 8, '60726-80003', 'CYTOSPORT', NULL),
(47, 16, 0, 0, 6, 7, 3, 'CSPTMUSC5LBSCNCRPW', 'cysmmcksncrm5lb-s.png', 0.0000, '2007-05-22 00:23:52', '2013-01-28 00:00:04', NULL, 5.60, 1, 1, 8, 0, '660726504260', 'cysmmcksncrm5lb-l.png', 63.0400, 91.49, 0, '1', 40.179, 'CSPTMUSC5LBSCNCRPW', 8, 8, 11, '60726-50426', 'CYTOSPOR-5', NULL),
(48, 70, 18, 2, 14, 31, 5, 'CSPTMMBR0008CHOCBR', 'cysmmbarchocpntcarml8ct-s.png', 0.0000, '2007-05-22 00:23:52', '2012-10-01 23:20:03', NULL, 1.45, 1, 1, 8, 0, '660726506219', 'cysmmbarchocpntcarml8ct-l.png', 15.3400, 23.92, 0, '1', 9.97, 'CSPTMMBR0008CHOCBR', 6, 6.5, 2, '660726506219', 'CYTOSPORT', NULL),
(49, 101, 11, 8, 45, 34, 3, 'CSPTMMBR0008VATOBR', 'cysmmbarvantoffcrnch8ct-s.png', 0.0000, '2007-05-22 00:23:52', '2013-01-28 00:00:04', NULL, 1.45, 1, 1, 8, 0, '660726506110', 'cysmmbarvantoffcrnch8ct-l.png', 15.3400, 23.92, 0, '1', 9.979, 'CSPTMMBR0008VATOBR', 6, 6.5, 2, '660726506110', 'CYTOSPORT', NULL),
(50, 29, -3, 41, -3, -3, -3, 'SHAKERCYTO000BOTLEA', 'cysmmshakerbtl-s.png', 0.0000, '2007-05-22 00:23:52', '2013-02-01 14:20:09', NULL, 0.25, 1, 1, 8, 0, '', 'cysmmshakerbtl-s.png', 2.9500, 4.95, 0, '1', 1.5, 'SHAKERCYTO000BOTLEA', 0, 0, 0, '', 'SHAKER', NULL),
(53, 18, -3, -3, 0, 23, 1, 'FREEEXTR0001GRAPLQ', 'freliqstuffgrp20oz-s.png', 0.0000, '2007-05-22 00:23:52', '2012-07-26 12:03:25', NULL, 1.35, 1, 1, 10, 0, '729609016020', 'freliqstuffgrp20oz-l.png', 11.7500, 19.95, 0, '1', 8.99, 'FREEEXTR0001GRAPLQ', 0, 0, 0, '181785', 'FREEDOM', NULL),
(54, 13, -3, -3, -3, 23, -1, 'FREEEXTR0001PUNCLQ', 'freliqstufffp20oz-s.png', 0.0000, '2007-05-22 00:23:52', '2012-10-04 01:00:06', NULL, 1.35, 1, 1, 10, 0, '729609016013', 'freliqstufffp20oz-l.png', 11.7500, 19.95, 0, '1', 8.99, 'FREEEXTR0001PUNCLQ', 2.5, 2.5, 8, '181795', 'FREEDOM', NULL),
(55, 3, 13, -3, -3, -1, -3, 'FORTNEUR0020GRPEPK', 'freliqstuffgrp16oz-s.png', 0.0000, '2007-05-22 00:23:52', '2012-08-10 17:00:04', NULL, 1.10, 1, 1, 10, 0, '729609016648', 'freliqstuffgrp16oz-l.png', 10.7500, 18.95, 0, '1', 7.75, 'FORTNEUR0020GRPEPK', 2.5, 2.5, 7.25, '', 'FREEDOM', NULL),
(56, 18, 13, -3, -3, 4, 7, 'FORTVITA0020FRTPPK', 'freliqstufffp16oz-s.png', 0.0000, '2007-05-22 00:23:52', '2012-08-10 17:00:04', NULL, 1.10, 1, 1, 10, 0, '729609016662', 'freliqstufffp16oz-l.png', 10.7500, 18.95, 0, '1', 7.75, 'FORTVITA0020FRTPPK', 2.5, 2.5, 7.25, '181791', 'FREEDOM', NULL),
(66, 307, 34, 182, 13, 37, 41, 'ISSOHYE0012CHOCBR', 'issohyeahchocar12bar-s.png', 0.0000, '2007-05-22 00:23:52', '2013-02-02 09:00:04', NULL, 2.75, 1, 1, 15, 0, '788434113519', 'issohyeahchocar12bar-l.png', 27.7200, 40.72, 0, '1', 17.414, 'ISSOHYE0012CHOCBR', 7, 8, 3, '01-100200', 'ISS', NULL),
(67, 179, 82, 27, 19, 24, 27, 'ISSOHYE0012PBSTBR', 'issohyeahpbnstrw12ct-s.png', 0.0000, '2007-05-22 00:23:52', '2013-02-02 09:00:04', NULL, 2.75, 1, 1, 15, 0, '788434113632', 'issohyeahpbnstrw12ct-l.png', 27.7200, 40.72, 0, '1', 17.507, 'ISSOHYE0012PBSTBR', 7, 8, 3, '01-100204', 'ISS', NULL),
(74, 82, 45, 5, 28, -3, 7, 'LABRLNBY0042CHOCPK', 'labcbleanchoc42ct-s.png', 0.0000, '2007-05-22 00:23:52', '2013-02-02 09:00:04', NULL, 8.00, 1, 1, 18, 0, '710779120068', 'labcbleanchoc42ct-l.png', 103.2100, 158.78, 0, '1', 58.231, 'LABRLNBY0042CHOCPK', 7.75, 11.5, 11, 'MR-0185', 'LABRADA', NULL),
(75, 139, 32, 9, 61, 31, 6, 'LABRLNBY0042VANIPK', 'labcbleanvan42ct-s.png', 0.0000, '2007-05-22 00:23:52', '2013-02-02 09:00:04', NULL, 8.00, 1, 1, 18, 0, '710779120051', 'labcbleanvan42ct-l.png', 103.2100, 158.78, 0, '1', 60.774, 'LABRLNBY0042VANIPK', 7.75, 11.5, 11, 'MR-0184', 'LABRADA', NULL),
(76, 6, 4, 1, 2, 2, -3, 'LABRMASS06LBCHOCPW', 'labmass60choc6lb-s.png', 0.0000, '2007-05-22 00:23:52', '2013-02-02 09:00:04', NULL, 6.65, 1, 1, 18, 0, '710779560277', 'labmass60choc6lb-l.png', 57.3000, 82.38, 0, '1', 33.839, 'LABRMASS06LBCHOCPW', 8, 8, 10, 'WG-0409', 'LABRADA', NULL),
(77, 7, 8, -1, 4, -1, -3, 'LABRMASS06LBVANIPW', 'LABRMASS06LBVANIPW-s.png', 0.0000, '2007-05-22 00:23:52', '2013-02-02 09:00:04', NULL, 6.45, 1, 1, 18, 0, '710779560260', 'LABRMASS06LBVANIPW-l.png', 57.3000, 82.38, 0, '1', 33.163, 'LABRMASS06LBVANIPW', 8, 8, 10, 'WG-0408', 'LABRADA', NULL),
(78, -15, -3, -3, -3, -3, -3, 'SHAKERLAB0001SMOOEA', 'labshakerbtl-s.png', 0.0000, '2007-05-22 00:23:52', '2013-02-06 11:20:09', NULL, 0.25, 1, 1, 18, 0, '', 'labshakerbtl-l.png', 3.0000, 7.99, 0, '1', 1.785, 'SHAKERLAB0001SMOOEA', 0, 0, 0, '', 'SHAKER', NULL),
(80, -15, -3, -3, -3, -3, -3, 'LEGAFORM00600000CP', 'lgsciencesformadrolextreme-s.png', 0.0000, '2007-05-22 00:23:52', '2012-11-19 23:00:04', NULL, 0.25, 0, 1, 19, 0, '897686002165', 'lgsciencesformadrolextreme-l.png', 32.9900, 54.99, 0, '1', 18.145, 'LEGAFORM00600000CP', 2.25, 2.25, 4.25, '', 'LEGAL', NULL),
(82, -15, -3, -3, -3, -3, -3, 'LEGAMET100750000CP', 'lgsciencesmethyl1d-s.png', 0.0000, '2007-05-22 00:23:52', '2013-02-21 14:12:13', NULL, 0.22, 0, 1, 19, 0, '897686002035', 'lgsciencesmethyl1d-l.png', 47.9900, 79.99, 0, '1', 26.4, 'LEGAMET100750000CP', 0, 0, 0, '', 'LEGAL', NULL),
(85, 811, 120, 17, 461, 74, 139, 'METXMEBC0012COOKBR', 'metbig100colsprckcrh-s.png', 0.0000, '2007-05-22 00:23:52', '2012-08-10 17:00:05', NULL, 2.95, 1, 1, 20, 0, '786560153133', 'metbig100colsprckcrh-l.png', 25.0000, 35.88, 0, '1', 17.76, 'METXMEBC0012COOKBR', 8, 7.25, 4.25, '15313', 'METX', NULL),
(86, 456, 89, 31, 225, 55, 56, 'METXMEBC0012APPLBR', 'metbig100colcpaplpie12bar-s.png', 0.0000, '2007-05-22 00:23:52', '2012-12-21 10:20:05', NULL, 3.05, 1, 1, 20, 0, '786560159364', 'metbig100colcpaplpie12bar-l.png', 25.0000, 35.88, 0, '1', 17.76, 'METXMEBC0012APPLBR', 8, 7.25, 4.25, '15936', 'METX', NULL),
(87, 499, 76, -3, 259, 51, 116, 'METXMEBC0012PEANBR', 'metbig100colpbcrcrch12bar-s.png', 0.0000, '2007-05-22 00:23:52', '2013-01-04 23:40:05', NULL, 2.75, 1, 1, 20, 0, '786560149518', 'metbig100colpbcrcrch12bar-l.png', 25.0000, 35.88, 0, '1', 17.76, 'METXMEBC0012PEANBR', 8, 7.25, 4.25, '14951', 'METX', NULL),
(88, 45, 13, 5, -3, 22, 8, 'MAXHABOM02240000TB', 'mhpabomb224tab-s.png', 0.0000, '2007-05-22 00:23:52', '2012-08-10 17:00:05', NULL, 1.00, 1, 1, 21, 0, '666222074405', 'mhpabomb224tab-l.png', 47.9900, 79.99, 0, '1', 31.2, 'MAXHABOM02240000TB', 3.5, 3.5, 6.5, '074405', 'MHP', NULL),
(89, 11, -1, 5, -3, 13, -3, 'MAXHCYCL01200000CP', 'mhpcyclin120ct-s.png', 0.0000, '2007-05-22 00:23:52', '2012-08-10 17:00:05', NULL, 0.35, 1, 1, 21, 0, '666222032306', 'mhpcyclin120ct-l.png', 47.9900, 79.99, 0, '1', 31.19, 'MAXHCYCL01200000CP', 2.75, 2.75, 4.75, '032306', 'MHP', NULL),
(93, 70, 6, 5, 16, 43, 0, 'MAXHPROB02LBCHOCPW', 'mhpprobsrchoc2lb-s.png', 0.0000, '2007-05-22 00:23:52', '2012-08-10 17:00:05', NULL, 2.40, 1, 1, 21, 0, '666222011318', 'mhpprobsrchoc2lb-l.png', 29.3900, 41.99, 0, '1', 19.1, 'MAXHPROB02LBCHOCPW', 6, 6, 10, '', 'MHP', NULL);

-- --------------------------------------------------------

--
-- Table structure for table `products_description`
--

CREATE TABLE IF NOT EXISTS `products_description` (
  `products_id` int(11) NOT NULL,
  `language_id` int(11) NOT NULL DEFAULT '1',
  `products_name` varchar(64) NOT NULL,
  `products_description` text,
  `products_url` varchar(255) DEFAULT NULL,
  `products_viewed` int(5) DEFAULT '0',
  `other_ingredients` text NOT NULL,
  `directions_of_use` text NOT NULL,
  `allergen_information` text NOT NULL,
  `warnings` text NOT NULL,
  `nutrition_facts_htm` varchar(255) NOT NULL,
  `nutrition_facts_image` varchar(255) NOT NULL
) ENGINE=MyISAM AUTO_INCREMENT=4624 DEFAULT CHARSET=latin1;

--
-- Dumping data for table `products_description`
--

INSERT INTO `products_description` (`products_id`, `language_id`, `products_name`, `products_description`, `products_url`, `products_viewed`, `other_ingredients`, `directions_of_use`, `allergen_information`, `warnings`, `nutrition_facts_htm`, `nutrition_facts_image`) VALUES
(9, 1, 'BSN: Lean Dessert Protein Chocolate Coconut  1.38 lb', '<p>Contains a Sustained Release Multi-Function Micallar Protein Matrix which provides a precision release of quick and slow digestion protein designed to encourage rapid muscle nitrogen retention followed by a controlled anabolic release of amino acids for up to 7 hours! This special formulated matrix contains the 6 most complete and effective protein sources available for enhancing lean muscle tissue! Designed for athletes and bodybuilders who need a superior protein matrix to help them meet their nutritional requirements and physique goals! Contains Bioactive Protein Utilization Enzymes that promote optimum protein digestion and increases amino acid uptake by the muscle. BSN''s advanced flavor technology replicates some of the most mouthwatering desserts you have ever tasted. Contains MCT''s, EFA''s, and Glutamine Peptides, which stimulate energy, fat loss, and lean muscle growth! Designed to be thick and satisfying (helping control your cravings and appetite until your next meal)!</p>', '', 11, '<p>A Sustained Release Multi-Functional Micellar Protein Matrix Comprised of (Ultrafiltered Whey Protein Concentrate [Milk] Containing Alpha-Lactalbumin, Micellar Alpha And Beta Caseins And Caseinates [Milk], Microfiltered Whey Protein Isolate [Milk] Containing Whey Isolate Peptide fractions, Egg Albumen [Egg], Calcium Caseinate [Milk], Milk Protein Concentrate [Milk]), Dutch Processed Cocoa, Maltodextrin, Corn Syrup Solids, Natural And Artificial Flavors, Vegetable Gum, Beta Carotene for Color, Xanthan Gum, Carrageenan, Sucralose, Mono & Di Glycerides, Sodium Alginate, And Lecithin [Soy]</p>', '<p>As a dietary supplement, take 2 heaping scoops with 8-10 oz. of cold water or any beverage of your choice. Vary the amount of liquid to achieve your desired consistency. Drink 2-4 servings daily, or as needed to satisfy your protein or muscle enchancing requirements. To increase your protein intake per serving and to achieve an amazing milk shake taste, use non-fat or low fat milk.</p>', '<p>Manufactured on equipment which processes products containing milk, egg, soybeans, shellfish, fish oil, tree nuts, and peanut flavor.</p>', '<p>Very low calorie protein diets may cause serious illness or death. Do not use for weight reduction in such diets without medical supervision. Not for use by infants, children, or pregnant or nursing women.</p>', 'BSNIDESS1.39CHCOPW.html', ''),
(10, 1, 'BSN: Lean Dessert Protein Whipped Vanilla Cream 1.38 lb', '<p>Contains a Sustained Release Multi-Function Micallar Protein Matrix which provides a precision release of quick and slow digestion protein designed to encourage rapid muscle nitrogen retention followed by a controlled anabolic release of amino acids for up to 7 hours! This special formulated matrix contains the 6 most complete and effective protein sources available for enhancing lean muscle tissue! Designed for athletes and bodybuilders who need a superior protein matrix to help them meet their nutritional requirements and physique goals! Contains Bioactive Protein Utilization Enzymes that promote optimum protein digestion and increases amino acid uptake by the muscle. BSN''s advanced flavor technology replicates some of the most mouthwatering desserts you have ever tasted. Contains MCT''s, EFA''s, and Glutamine Peptides, which stimulate energy, fat loss, and lean muscle growth! Designed to be thick and satisfying (helping control your cravings and appetite until your next meal)!</p>', '', 14, '<p>A Sustained Release Multi-Functional Micellar Protein Matrix Comprised of (Ultrafiltered Whey Protein Concentrate [Milk] Containing Alpha-Lactalbumin, Micellar Alpha And Beta Caseins And Caseinates [Milk], Microfiltered Whey Protein Isolate [Milk] Containing Whey Isolate Peptide fractions, Egg Albumen [Egg], Calcium Caseinate [Milk], Milk Protein Concentrate [Milk]), Maltodextrin, Corn Syrup Solids, Natural And Artificial Flavors, Vegetable Gum, Beta Carotene for Color, Xanthan Gum, Carrageenan, Sucralose, Mono & Di Glycerides, Sodium Alginate, And Lecithin [Soy]</p>', '<p>As a dietary supplement, take 2 heaping scoops with 8-10 oz. of cold water or any beverage of your choice. Vary the amount of liquid to achieve your desired consistency. Drink 2-4 servings daily, or as needed to satisfy your protein or muscle enchancing requirements. To increase your protein intake per serving and to achieve an amazing milk shake taste, use non-fat or low fat milk.</p>', '<p>Manufactured on equipment which processes products containing milk, egg, soybeans, shellfish, fish oil, tree nuts, and peanut flavor.</p>', '<p>Very low calorie protein diets may cause serious illness or death. Do not use for weight reduction in such diets without medical supervision. Not for use by infants, children, or pregnant or nursing women.</p>', 'BSNIDESS1.39VANNPW.html', ''),
(16, 1, 'Champion Nutrition: Ultramet Chocolate 60 ct', '<p>If it''s difficult to take a break during a hectic day and treat yourself to a nice meal, and if you feel you deserve something more than a quick junk food snack, then why not have an UltraMet shake? UltraMet is the delicious shake that only takes seconds to prepare and supplies more protein (over 80% of the Recommended Daily Value) and more nutrients (27 essential vitamins and minerals) for less money than many high-calorie fast-food meals or virtually any other food you could name. It''s a delicious way to feel great! UltraMet is a high-performance meal supplement that delivers a complete array of nutrients to fuel your body, feed your muscles, help improve your overall well-being and boost your body''s energy levels. It''s ideal for anyone who is looking to get quick energy and superior nutrition in a glass. One serving of UltraMet contains a blend of up to 42 grams of easily-digested protein, which delivers a full spectrum of the 9 essential amino acids, only 3 grams of fat and just enough healthy complex carbohydrates to stabilize your blood sugar and satisfy you for hours. Try getting that from a burger and some fries. </p>', '', 10, '<p>Peptol Ex (proprietary blend consisting of: ion exchange whey protein isolate, calcium caseinate, milk protein isolate, potassium caseinate, egg albumen, whey protein concentrate, sodium caseinate, L-glutamine, taurine, and calcium alpha-ketoglutarate) maltodextrin, natural and artificial flavoring, fructose, vitamin and mineral blend consisting of ( dicalcium phosphate, magnesium oxide, potassium phosphate, potassium citrate, choline bitartrate, potassium chloride, vitamin e acetate, ascorbic acid, ferrous fumarate, beta carotene, boron proteinate, biotin, niacinamide, zinc oxide, manganese gluconate, vitamin a palmitate, calcium pantothenate, molybdenum amino acid chelate, copper gluconate, folic acid, vitamin d3, copper sulfate, pyridoxine hydrochloride, thiamine mononitrate, riboflavin, chromium polynicotinate, selenium amino acid chelate, potassium iodide, cyanocobalamin, canola oil, salt, soy lecithin, xantahan gum, cellulose gum, aspartame, citrimax (garcomoa cambogia extract) medium chain triglycerides, carrageenan, bromelain, borage oil, acesulfame potassium, mono and diglycerides. contains milk, egg, and soy. </p>', '<p>For a deliciously thick shake, mix contents of packed with 12-15 oz. of cold water and blend thoroughly for 45 seconds. Use 1-2 servings a day. </p>', '', '', 'CHMPNUTR0060CHOCPK.html', ''),
(17, 1, 'Champion Nutrition: Ultramet Low Carb Chocolate Fudge 60 ct', '<p>Whether you are a serious athlete trying to improve your lean body mass, or a busy mother trying to eat better and reduce body fat, UltraMet Low Carb is complete nutrition for anyone trying to reduce the carb content in their diet. Each serving of UltraMet Low Carb delivers only 6.5 g of carbs (4.5 g net carbs), plus essential fatty acids, vitamins, minerals, fiber, micro-nutrients and a whopping 35 g of protein! UltraMet Low Carb also helps you burn your existing fat stores while reducing the conversion of carbs into fat. If you run short of protein on your low-carb diet, your body will begin to consume your muscles for energy. Your metabolism will slow down. The extra high-quality protein and amino acids in UltraMet Low Carb help to protect your muscles and keep your metabolism going strong so you can burn fat and stay strong. Many people say that a low-carb diet means a high-fat diet because some low-carb diets include irresponsible levels of unhealthy fats. UltraMet Low Carb contains only 6.5 g of fat! This is a minimal amount and important for appetite control and health, particularly when you are trying to control your weight.</p>', '', 0, '<p>Peptol Ex-LC(proprietary blend consisting of: ion exchange whey protein isolate, calcium caseinate, milk protein isolate, potassium caseinate, egg albumen, whey protein concentrate, sodium caseinate, L-glutamine, taurine, and calcium alpha-ketoglutarate) maltodextrin, low fat dutch cocoa,natural and artificial flavoring, fructose, vitamin and mineral blend consisting of ( dicalcium phosphate, magnesium oxide, potassium phosphate, potassium citrate, choline bitartrate, potassium chloride, vitamin e acetate, ascorbic acid, ferrous fumarate, beta carotene, boron proteinate, biotin, niacinamide, zinc oxide, manganese gluconate, vitamin a palmitate, calcium pantothenate, molybdenum amino acid chelate, copper gluconate, folic acid, vitamin d3, copper sulfate, pyridoxine hydrochloride, thiamine mononitrate, riboflavin, chromium polynicotinate, selenium amino acid chelate, potassium iodide, cyanocobalamin, canola oil, salt, soy lecithin, xantahan gum, cellulose gum, aspartame, citrimax (garcomoa cambogia extract) medium chain triglycerides, carrageenan, bromelain, borage oil, acesulfame potassium, mono and diglycerides. contains milk, egg, and soy. </p>', '<p>For a deliciously thick shake, mix contents of packed with 12-15 oz. of cold water and blend thoroughly for 45 seconds. Use 1-2 servings a day.</p>', '', '', 'CHMPNULC0060CHOCPK.html', ''),
(18, 1, 'Champion Nutrition: Ultramet Low Carb Vanilla Cream 60 ct', '<p>Whether you are a serious athlete trying to improve your lean body mass, or a busy mother trying to eat better and reduce body fat, UltraMet Low Carb is complete nutrition for anyone trying to reduce the carb content in their diet. Each serving of UltraMet Low Carb delivers only 6.5 g of carbs (4.5 g net carbs), plus essential fatty acids, vitamins, minerals, fiber, micro-nutrients and a whopping 35 g of protein! UltraMet Low Carb also helps you burn your existing fat stores while reducing the conversion of carbs into fat. If you run short of protein on your low-carb diet, your body will begin to consume your muscles for energy. Your metabolism will slow down. The extra high-quality protein and amino acids in UltraMet Low Carb help to protect your muscles and keep your metabolism going strong so you can burn fat and stay strong. Many people say that a low-carb diet means a high-fat diet because some low-carb diets include irresponsible levels of unhealthy fats. UltraMet Low Carb contains only 6.5 g of fat! This is a minimal amount and important for appetite control and health, particularly when you are trying to control your weight. </p>', '', 0, '<p>Peptol Ex-LC(proprietary blend consisting of: ion exchange whey protein isolate, calcium caseinate, milk protein isolate, potassium caseinate, egg albumen, whey protein concentrate, sodium caseinate, L-glutamine, taurine, and calcium alpha-ketoglutarate) maltodextrin,natural and artificial flavoring, fructose, vitamin and mineral blend consisting of ( dicalcium phosphate, magnesium oxide, potassium phosphate, potassium citrate, choline bitartrate, potassium chloride, vitamin e acetate, ascorbic acid, ferrous fumarate, beta carotene, boron proteinate, biotin, niacinamide, zinc oxide, manganese gluconate, vitamin a palmitate, calcium pantothenate, molybdenum amino acid chelate, copper gluconate, folic acid, vitamin d3, copper sulfate, pyridoxine hydrochloride, thiamine mononitrate, riboflavin, chromium polynicotinate, selenium amino acid chelate, potassium iodide, cyanocobalamin, canola oil, salt, soy lecithin, xantahan gum, cellulose gum, aspartame, citrimax (garcomoa cambogia extract) medium chain triglycerides, carrageenan, bromelain, borage oil, acesulfame potassium, mono and diglycerides. contains milk, egg, and soy. </p>', '<p>For a deliciously thick shake, mix contents of packed with 12-15 oz. of cold water and blend thoroughly for 45 seconds. Use 1-2 servings a day. </p>', '', '', 'CHMPNULC0060VANIPK.html', ''),
(19, 1, 'Champion Nutrition: Ultramet Strawberry 60 ct', '<p>If you are like most people, you don''t have the time to spend hours planning and preparing healthy meals. But you may spend hours in the mirror not satisfied with what you see. If you exercise regularly but aren''t getting the results you really want, it''s likely your diet is working against you. One shake of Ultramet gives you 80% of your daily protein requirements with only 2 grams of fat. Try getting that from a hamburger. In addition you get 50% of your recommended daily intake for over 18 essential vitamins and minerals. Drop 12 - 15 oz of cold water and a packet of Ultramet into a blender or shaker and you have a quick and efficient meal. Forget the stove, the pots, the pans, and the over-sized clothing. Let''s face it, sitting around and dreaming isn''t going to get you the body you want. Instead, combine 2 to 4 Ultramet shakes a day with exercise and a sensible diet and you are well on your way to enjoying your new body. Give your body what it needs and it will repay you by looking its best. </p>', '', 0, '<p>Peptol Ex (proprietary blend consisting of: ion exchange whey protein isolate, calcium caseinate, milk protein isolate, potassium caseinate, egg albumen, whey protein concentrate, sodium caseinate, L-glutamine, taurine, and calcium alpha-ketoglutarate) maltodextrin, natural and artificial flavoring, fructose, vitamin and mineral blend consisting of ( dicalcium phosphate, magnesium oxide, potassium phosphate, potassium citrate, choline bitartrate, potassium chloride, vitamin e acetate, ascorbic acid, ferrous fumarate, beta carotene, boron proteinate, biotin, niacinamide, zinc oxide, manganese gluconate, vitamin a palmitate, calcium pantothenate, molybdenum amino acid chelate, copper gluconate, folic acid, vitamin d3, copper sulfate, pyridoxine hydrochloride, thiamine mononitrate, riboflavin, chromium polynicotinate, selenium amino acid chelate, potassium iodide, cyanocobalamin, canola oil, salt, soy lecithin, xantahan gum, cellulose gum, aspartame, citrimax (garcomoa cambogia extract) medium chain triglycerides, carrageenan, bromelain, borage oil, acesulfame potassium, mono and diglycerides. contains milk, egg, and soy. </p>', '<p>For a deliciously thick shake, mix contents of packed with 12-15 oz. of cold water and blend thoroughly for 45 seconds. Use 1-2 servings a day. </p>', '', '', 'CHMPNUTR0060STRAPK.html', ''),
(20, 1, 'Champion Nutrition: Ultramet Vanilla 60 ct', '<p>If it''s difficult to take a break during a hectic day and treat yourself to a nice meal, and if you feel you deserve something more than a quick junk food snack, then why not have an UltraMet shake? UltraMet is the delicious shake that only takes seconds to prepare and supplies more protein (over 80% of the Recommended Daily Value) and more nutrients (27 essential vitamins and minerals) for less money than many high-calorie fast-food meals or virtually any other food you could name. It''s a delicious way to feel great! UltraMet is a high-performance meal supplement that delivers a complete array of nutrients to fuel your body, feed your muscles, help improve your overall well-being and boost your body''s energy levels. It''s ideal for anyone who is looking to get quick energy and superior nutrition in a glass. One serving of UltraMet contains a blend of up to 42 grams of easily-digested protein, which delivers a full spectrum of the 9 essential amino acids, only 3 grams of fat and just enough healthy complex carbohydrates to stabilize your blood sugar and satisfy you for hours. Try getting that from a burger and some fries. </p>', '', 0, '<p>Peptol Ex (proprietary blend consisting of: ion exchange whey protein isolate, calcium caseinate, milk protein isolate, potassium caseinate, egg albumen, whey protein concentrate, sodium caseinate, L-glutamine, taurine, and calcium alpha-ketoglutarate) maltodextrin, natural and artificial flavoring, fructose, vitamin and mineral blend consisting of ( dicalcium phosphate, magnesium oxide, potassium phosphate, potassium citrate, choline bitartrate, potassium chloride, vitamin e acetate, ascorbic acid, ferrous fumarate, beta carotene, boron proteinate, biotin, niacinamide, zinc oxide, manganese gluconate, vitamin a palmitate, calcium pantothenate, molybdenum amino acid chelate, copper gluconate, folic acid, vitamin d3, copper sulfate, pyridoxine hydrochloride, thiamine mononitrate, riboflavin, chromium polynicotinate, selenium amino acid chelate, potassium iodide, cyanocobalamin, canola oil, salt, soy lecithin, xantahan gum, cellulose gum, aspartame, citrimax (garcomoa cambogia extract) medium chain triglycerides, carrageenan, bromelain, borage oil, acesulfame potassium, mono and diglycerides. contains milk, egg, and soy. </p>', '<p>For a deliciously thick shake, mix contents of packed with 12-15 oz. of cold water and blend thoroughly for 45 seconds. Use 1-2 servings a day. </p>', '', '', 'CHMPNUTR0060VANLPK.html', ''),
(22, 1, 'Clif Bar: Blueberry Crisp 12 ct', '<p>We want a simple approach in an energy bar not complicated formulas or gimmicks. Don''t you? It''s just an approach that makes sense. CLIF BARS are a great-tasting, convenient source of energy for any activity. To perform at your best, the last thing you want to put in your body is a synthetic product. That''s why we believe in using healthy ingredients, such as organic rolled oats, organic roasted soybeans, organic milled flaxseed and organic soy flour. All 15 CLIF BAR flavors are made with 70% organic ingredients. It''s your body, you decide. With a mix of carbohydrates, protein, and fiber, CLIF BAR is a moderate glycemic index food. What does this mean? Basically, you''ll benefit from a steady increase in blood sugar levels followed by a gradual decline (sustained energy!). </p>\r\n\r\n<p><b><FONT COLOR="FF0000">Due to temperature conditions in the Summer, this product may melt during the shipping process.</FONT></b></p>', '', 1, '<p>Organic Brown Rice Syrup, ClifPro (Soy Rice Crisps [Soy Protein Isolate, Rice Flour, Malt Extract], Organic Roasted Soybeans, Organic Soy Flour), Blueberry Fruit Pieces (Organic Evaporated Cane Juice, Blueberry Puree Concentrate, Brown Rice Syrup, Pectin, Citric Acid, Sodium Citrate, Natural Flavor), Organic Rolled Oats, Organic Dry Roasted Almonds, Organic Evaporated Cane Juice, Organic Toasted Oats (Organic Oats, Organic Evaporated Cane Juice), ClifCrunch (Organic Oat Fiber, Inulin [Chicory Extract], Organic Milled Flaxseed, Organic Oat Bran, Psyllium), Blueberries (Blueberries, Apple Juice Concenrate), Organic Date Paste, Almond Butter, Organic Sunflower Oil, Natural Flavors, Sea Salt, Citric Acid. Vitamins & Minerals: Dicalcium Phosphate, Magnesium Oxide, Ascorbic Acid (VIt.C), Tocopheryl Acetate (Vit.E), Ferric Orthophosphate (Iran), Beta Carotene (Vit.A), Zinc Citrate, Phytonadione (Vit.K1), Biotin, Niacinamide (VIt.B3), Calcium Pantothenate (Vit.B5), Potassium Iodide, Manganese Gluconate, Copper Gluconate, Sodium Selenite, Thiamin (Vit.B1), Chromium Chloride, Cyanocobalamin (Vit.B12), Sodium Molybdate, Folic Acid (Vit.B9), Riboflavin (VIt.B2), Pyridoxine Hydrochloride (Vit.B6).\r\n</p>', '', '<p>CONTAINS SOY AND WALNUTS. MAY CONTAIN TRACES OF DAIRY AND TREE NUTS. WE SOURCE INGREDIENTS WHICH DO NOT CONTAIN WHEAT, DAIRY, AND ARE NOT GENETICALLY ENGINEERED. </p>', '', 'CLIFCLBR0012BLUEBR.html', ''),
(23, 1, 'Clif Bar: Builder''s Chocolate Peanut Butter 12 ct', '<p>CLIF&reg; Builder''s is the first delicious high protein bar to pay tribute to those who turn their everyday world into a gym. CLIF&reg; Builder''s will fuel your muscles with added protein, especially when your day requires a little extra oomph. CLIF&reg; Builder''s - The uniquely crispy and chewy, high protein bar made with entirely natural ingredients. With 20 grams of the best quality soy and nut proteins, Builder''s is the healthy and convenient way to get more protein into your diet. Whether you''re hankering for a convenient and delicious snack that satisfies hunger, or you''re interested in muscle health and recovery after physical activity, CLIF&reg; Builder''s delivers great taste and sound nutrition.</p>\r\n\r\n<p><b><FONT COLOR="FF0000">Due to temperature conditions in the Summer, this product may melt during the shipping process.</FONT></b></p>', '', 0, '<p>Soy protein isolate, beet juice concentrate, organic brown rice syrup, organic evaporated cane juice, palm kernel oil, dry roasted peanuts, organic rolled oats, organic soy protein concentrate, cocoa, vegetable glycerin, natural flavors, peanut flour, rice starch, inulin (chicory extract), cocoa powder, salt, organic milled flaxseed, organic oat fiber, soy lecithin, organic sunflower oil. Vitamin &amp; Minerals: dicalcium phosphate, magnesium oxide, ascorbic acid (Vit, C), tocopheryl acetate (Vit E), ferric orthophosphate (Iron), beta carotene (Vit. A), zinc citrate, phytonadione (Vit. K1), biotin, niacinamide (Vit. B3), calcium pantothenate (Vit. B5), potassium iodide, manganese gluconate, copper gluconate, sodium selenite, thiamin (Vit. B1), chromium chloride, cyanocobalamin (Vit. B12), sodium molybdate, folic acid (Vit. B9), riboflavin (Vit. B2), pyridoxine hydrochloride (Vit. B6) 35% organic ingredients</p>', '<p>CLIF Builder''s is a nutritious and convenient source of protein after a workout or as a snack. It is not a meal replacement bar.</p>', '<p>Contains soy and peanuts. May contain traces of dairy, wheat and tree nuts. We source ingredients that are not genetically engineered.</p>', '', 'CLIFBUIL0012PEANBR.html', ''),
(24, 1, 'Clif Bar: Chocolate Chip 12 ct', '<p>We want a simple approach in an energy bar not complicated formulas or gimmicks. Don''t you? It''s just an approach that makes sense. CLIF BARS are a great-tasting, convenient source of energy for any activity. To perform at your best, the last thing you want to put in your body is a synthetic product. That''s why we believe in using healthy ingredients, such as organic rolled oats, organic roasted soybeans, organic milled flaxseed and organic soy flour. All 15 CLIF BAR flavors are made with 70% organic ingredients. It''s your body, you decide. With a mix of carbohydrates, protein, and fiber, CLIF BAR is a moderate glycemic index food. What does this mean? Basically, you''ll benefit from a steady increase in blood sugar levels followed by a gradual decline (sustained energy!).</p>\r\n\r\n<p><b><FONT COLOR="FF0000">Due to temperature conditions in the Summer, this product may melt during the shipping process.</FONT></b></p>', '', 0, '<p>Organic Brown Rice Syrup, ClifPro (Soy Rice Crisps {Soy Protein Isolate, Rice Flour, Barley Malt Extract}, Organic Roasted Soybeans, Organic Soy Flour), Organic Rolled Oats, Organic Toasted Oats (Organic Oats, Organic Dried Cane Syrup), Chocolate Chips (Dried Cane Juice, Unsweetened Chocolate, Cocoa Butter, Soy Lecithin, Vanilla Extract), Organic Cane Syrup, ClifCrunch (Organic Oat Fiber, Apple Fiber, Inulin {Chicory Extract} Organic Milled Flaxseed, Psyllium), Organic Date Paste, Organic Soy Butter (Organic Roasted Soybeans, Organic Soybean Oil, Salt), Organic Sunflower Oil, Molasses Powder, Sea Salt, Natural Flavors, Cinnamon. Vitamins &amp; Minerals: Dicalcium Phosphate, Calcium Carbonate, Ascorbic Acid (Vit. C), Magnesium Oxide, DL-Alpha Tocopheryl Acetate (Vit. E), Ferric Orthophosphate (Iron), Zinc Citrate, Biotin, Beta Carotene (Vit. A), Niacinamide (Vit. B3), D-Calcium Pantothenate (Vit. B5), Copper Gluconate, Phytonadione (Vit. K1), Manganese Gluconate, Folic Acid (Vit. B9), Riboflavin (Vit. B2), Sodium Selenite, Thiamin (Vit. B1), Pyridoxine Hydrochloride (Vit. B6), Ergocalciferol (Vit. D2), Cyanocobalamin (Vit. B12), Chromium Chloride, Potassium Iodide. 70% organic ingredients</p>', '<p>For best results, CLIF BARs should be eaten about one to three hours before exercise along with water to prevent hunger and supply energy to working muscles. During prolonged, lower intensity exercise such as hiking or bike touring, CLIF BARs can be eaten during activity to help satiate hunger and meet carbohydrate demands. CLIF BARs can also be eaten after exercise, as a snack between meals or during a long, busy day to help sustain energy.</p>', '<p>Contains soy. May contain traces of dairy, peanuts, wheat, and tree nuts. We source ingredients that are not genetically engineered.</p>', '', 'CLIFCLBR0012CCHPBR.html', ''),
(25, 1, 'Clif Bar: Luna Bar Iced Oatmeal Raisin 15 ct', '<p>Why create a bar only for women? Many women fail to get adequate vitamins and minerals on a daily basis. LUNA bar was created to help women get more of the nutrients often lacking in their meal plans while being 100% natural and as organic as possible. That''s why LUNA bar contains ingredients, like folic acid (for healthy babies and mothers-to-be), soy protein (for a healthy heart), calcium (for strong bones), and 23 vitamins and minerals that are essential to a woman''s everyday nutritional needs. As women ourselves, we understand the importance of maintaining overall health and wellness. How does LUNA compare with other bars? LUNA was the first bar specifically formulated with a woman''s needs in mind. In just 180 calories, LUNA is entirely natural and 70% organic and meets many of the specific nutritional requirements women need to maintain active healthy lifestyles. LUNA bars also have no artificial sweeteners, no high fructose corn syrup and no trans fats. LUNA is dedicated to helping women meet the specific nutritional requirements to maintain active lifestyles. We take a whole-life approach, trying to embrace healthy living and bring communities of women together. When we feel well-nourished, we are more in tune with ourselves and the world around us. </p>\r\n\r\n<p><b><FONT COLOR="FF0000">Due to temperature conditions in the Summer, this product may melt during the shipping process.</FONT></b></p>', '', 0, '<p>LunaPro  ( Soy Rice Crisp [Soy Protein Isolate, Rice Flour],  Organic Oats, Organic Soy Flour, Organic Roasted Soybeans, Organic Milled Flaxseed), Brown Rice Syrup, Coating  (Organic Evaporated Cane Juice, Palm Kernel Oil, Cocoa, Inulin, Soy Lecithin, Natural Vanilla), Vegetable Glyercin, Organic Peanut Butter, Inulin, Peanut Flour, Natural Flavors, Sea Salt, Green Tea Extract. </p>', '', '<p>CONTAINS SOY & PEANUTS; MAY CONTAIN TRACES OF OTHER NUTS & DAIRY- NO HYDROGENATED OILS- WE SOURCE INGREDIENTS WHICH DO NOT CONTAIN WHEAT, DAIRY OR GMOs- 25% ORGANIC INGREDIENTS\r\n</p>', '', 'CLIFLUNA0015CARABR.html', ''),
(37, 1, 'CytoSport: Cytomax Cool Citrus 1.5 lb', '<p>Cytomax is the world''s most scientifically advanced complex carbohydrate, electrolyte performance energy drink. Ensures proper hydration, steady energy and reduced fatigue during exercise. Benefits of using Cytomax: Beats The Burn: Cytomax''s patented alpha L-Polylactate buffers lactic acid production in your muscles, reducing the &quot;burn&quot; during intense training and minimizing post-exercise muscle soreness. Steady Energy: Complex carbs stabilize blood sugar during exercise - no sugar crash. Cell Protection: antioxidants prevent exercise-induced free radical damage to muscle cells. Oxygen Delivery: succinates enhance oxygen delivery by up to 11% during exercise, reducing perceived exertion and minimizing muscle damage. Cellular Balance: Electrolytes ensure cellular nutrient balance is restored quickly after exercise. Low Osmoality: ensures fluid reaches your muscles quickly - as fast as water! Natural Herbal &quot;Lift&quot;: (in Tangy Orange, Go Grape and Peachy Keen flavors only) reduces your perceived effort so you can push harder. Best ways to use Cytomax: Before/During: Mix 1-1 1/2 scoops of Cytomax per 16 ounces of water to ensure a proper carbohydrate solution. Drink about 15 minutes before training and consistently during your workout. To ensure proper hydration, drink 32 ounces per hour of exercise. After: Drink a post-workout bottle for recovery and rehydration.\r\n</p>', '', 22, '<p>Cytosport''s Unique Complex Carbohydrate Blend Including Amylopectin Starches and Maltodextrins, Crystalline Fructose, Dextrose, Alpha-L-Polylactate (our Patented L-Lactate Formulation Containing Non-Acidic L-Lactate Ionically Bound to L-Arginine), Citric Acid, Natural and Artificial Flavor, Malic Acid, Sodium Citrate, Potassium Citrate, Guarana (Paullinia Cupana) Seed Extract, Beta Carotene, Ascorbic Acid, L-Alamine, L-Glutamine, Acesulfame Potassium, Guar Gum, Xanthan Gum, Magnesium Oxide, Turmeric Powder, Dicalcium Phosphate, Chromium Nicotinate, Monopotassium Phosphate, Magnesium Succinate, Potassium Succinate, Calcium Succinate</p>', '<p>Mix one to one and a half scoops of CYTOMAX in 16 oz. of water. Begin drinking 15-30 minutes into exercise. Drink 5 oz. every 15 minutes. After training, slowly drink another 16 oz. over the next hour to aid in recovery.\r\n</p>', '', '', 'CSPTCMAX01.5CITRPW.html', ''),
(39, 1, 'CytoSport: Cytomax Tangy Orange 1.5 lb', '<p>Cytomax is the world''s most scientifically advanced complex carbohydrate, electrolyte performance energy drink. Ensures proper hydration, steady energy and reduced fatigue during exercise. Benefits of using Cytomax: Beats The Burn: Cytomax''s patented alpha L-Polylactate buffers lactic acid production in your muscles, reducing the "burn" during intense training and minimizing post-exercise muscle soreness. Steady Energy: Complex carbs stabilize blood sugar during exercise - no sugar crash. Cell Protection: antioxidants prevent exercise-induced free radical damage to muscle cells. Oxygen Delivery: succinates enhance oxygen delivery by up to 11% during exercise, reducing perceived exertion and minimizing muscle damage. Cellular Balance: Electrolytes ensure cellular nutrient balance is restored quickly after exercise. Low Osmoality: ensures fluid reaches your muscles quickly - as fast as water! Natural Herbal "Lift": (in Tangy Orange, Go Grape and Peachy Keen flavors only) reduces your perceived effort so you can push harder. Best ways to use Cytomax: Before/During: Mix 1-1 1/2 scoops of Cytomax per 16 ounces of water to ensure a proper carbohydrate solution. Drink about 15 minutes before training and consistently during your workout. To ensure proper hydration, drink 32 ounces per hour of exercise. After: Drink a post-workout bottle for recovery and rehydration.</p>', '', 0, 'Cytosport''s Unique Complex Carbohydrate Blend Including Amylopectin Starches and Maltodextrins, Crystalline Fructose, Dextrose, Alpha-L-Polylactate (our Patented L-Lactate Formulation Containing Non-Acidic L-Lactate Ionically Bound to L-Arginine), Citric Acid, Natural and Artificial Flavor, Malic Acid, Sodium Citrate, Potassium Citrate, Guarana (Paullinia Cupana) Seed Extract, Beta Carotene, Ascorbic Acid, L-Alamine, L-Glutamine, Acesulfame Potassium, Guar Gum, Xanthan Gum, Magnesium Oxide, Turmeric Powder, Dicalcium Phosphate, Chromium Nicotinate, Monopotassium Phosphate, Magnesium Succinate, Potassium Succinate, Calcium Succinate', 'Mix one to one and a half scoops of CYTOMAX in 16 oz. of water. Begin drinking 15-30 minutes into exercise. Drink 5 oz. every 15 minutes. After training, slowly drink another 16 oz. over the next hour to aid in recovery.', '', '', 'CSPTCMAX01.5ORGEPW.html', ''),
(41, 1, 'CytoSport: EvoPro Vanilla 2.40 lb', '<p><strong>EvoPro&#8482;</strong> is the protein inside Muscle Milk; America’s favorite protein lean muscle formula. EvoPro&#8482;contains 25 grams of the same powerful proteins, while limiting the fats and carbohydrates. EvoPro&#8482; was formulated by the ultimate protein designer, nature, to help maximize muscle growth and recovery.</p>', '', 0, '<p><strong>EvoPro&#8482;</strong> is the protein inside Muscle Milk; America’s favorite protein lean muscle formula. EvoPro&#8482;contains 25 grams of the same powerful proteins, while limiting the fats and carbohydrates. EvoPro&#8482; was formulated by the ultimate protein designer, nature, to help maximize muscle growth and recovery.</p>', '<p>Mix one scoop in four to six ounces of water (less water makes a thicker shake). Your protein requirements may vary depending upon gender, body weight, level of activity, workout/training intensity, total diet and recovery needs. </p>\r\n<p>For best results in conjunction with intense weight training, take four or five servings of EVOPRO spaced evenly throughout the day.</p>\r\n<ul>\r\n  <li> one serving upon rising </li>\r\n  <li>one serving at mid-morning </li>\r\n  <li>one-two servings immediately after workout</li>\r\n  <li> one serving before bedtime</li>\r\n</ul>', '<p>Contains Ingredients Derived From Milk And Soy.</p>', '<p>This Product is Manufactured in a facility that Process Milk,Soy, Wheat and Eggs.</p>\r\n<p>*These statements have not been evaluated by the Food and Drug Administration. This product is not intended to diagnose, treat, cure, or prevent any disease.</p>', 'CSPTEVOP2.25VANNPW.html', ''),
(42, 1, 'CytoSport: Fast Twitch Power Punch 2.04 lb', '<p>CytoSport Fast Twitch is a power workout drink designed to give users explosive strength performance and fuel fast twitch (type II) fibers. It helps to amplify nitric oxide levels for total body pumps to help you protect lean muscle mass. Fast Twitch also has an endo-thermogenic effect which maximizes fat burning after your tough workouts. Awesome muscular explosiveness and size, formerly reserved for the genetically gifted, is the goal of Fast Twitch. Fast Twitch is the first formula specifically designed to fuel fast twitch (type II) muscle fibers for greater muscular power and gains. Fast Twitch helps increase post workout intensity, while enhancing muscular repair. The harder the workout, the more Fast Twitch can help. With Fast Twitch, you may run out of gym time before you run out muscle energy. Fueling type II fibers increases explosive power and speed. This translates into more sets, reps and weight lifted. Plus, type II fibers have greater potential for overall muscle hypertrophy (growth). </p>', '', 1, '<p>CytoCarb II (CytoSport''s unique combination of branching as well as short, medium and long linear chain maltodextrins with very low "DE"" (Dextrose Equivalence) providing a complex carbohydrate source approximately 96.5% Sugar-free), Citric Acid, Natural and Artificial Flavor, Glucuronolactone, Malic Acid, Sodium Bicarbonate, Acesulfame Potassium, Xanthan Gum, Guar Gum, Carrageenan, Sucralose, Red Carmine (added for color).\r\n</p>', '<p>Mix one scoop of FAST TWITCH in 16 oz of water. Drink 3/4 of bottle 30-45 minutes before workout. Drink remaining 1/4 bottle during workout. Best results are obtained when taken on empty stomach. Take on training days. Drink 64 fl oz of water daily. After workouts, take Muscle Milk within 60 minutes for fast recovery.\r\n  FAST TWITCH fuels increased explosive strength, while promoting muscular repair post workout. Our pH balanced creatine mix remains stable, so you can drink FAST TWITCH during workouts to keep muscle fibers firing harder, longer. </p>', '<p>Manufactured in a plant that processes milk, soy, wheat & eggs.\r\n</p>', '<p>Do not use if you are under the age of 18 or elderly. Do not take with any other stimulants or weight-loss supplement or any prescription or over the counter medicine. Do not use if you are pregnant or nursing or at risk of being treated for high-blood pressure, heart disease, hyperthyroidism, spasms, psychiatric disease, suffer from migraines, have asthma, or are taking asthma medication. Discontinue use if you experience dizziness, headache, nausea, or heart palpitations. If you have trouble sleeping, do not take within 6 hours of bedtime. Keep out of reach of children. </p>', 'CSPTFAST2.04PUNCPW.html', ''),
(43, 1, 'CytoSport: Fast Twitch Go Grape 2.04 lb', '<p>CytoSport Fast Twitch is a power workout drink designed to give users explosive strength performance and fuel fast twitch (type II) fibers. It helps to amplify nitric oxide levels for total body pumps to help you protect lean muscle mass. Fast Twitch also has an endo-thermogenic effect which maximizes fat burning after your tough workouts. Awesome muscular explosiveness and size, formerly reserved for the genetically gifted, is the goal of Fast Twitch. Fast Twitch is the first formula specifically designed to fuel fast twitch (type II) muscle fibers for greater muscular power and gains. Fast Twitch helps increase post workout intensity, while enhancing muscular repair. The harder the workout, the more Fast Twitch can help. With Fast Twitch, you may run out of gym time before you run out muscle energy. Fueling type II fibers increases explosive power and speed. This translates into more sets, reps and weight lifted. Plus, type II fibers have greater potential for overall muscle hypertrophy (growth). </p>', '', 0, '<p>"CytoCarb II (CytoSport''s unique combination of branching as well as short, medium and long linear chain maltodextrins with very low ""DE"" (dextrose equivalence) providing a complex carbohydrate source approximately 96.5% sugar-free), citric acid, natural and artificial flavor, glucuronolactone, malic acid, sodium bicarbonate, acesulfame potassium, xanthan gum, guar gum, carrageenan, red cabbage powder (added for color), sucralose."</p>', '<p>Mix one to three scoops/servings of FAST TWITCH in 16 oz of water. Drink 3/4 of bottle 30-45 minutes before workout. Drink remaining 1/4 bottle during workout. Best results are obtained when taken on empty stomach. Take on training days. Drink 64 fl oz of water daily. After workouts, take Muscle Milk within 60 minutes for fast recovery. FAST TWITCH fuels increased explosive strength, while promoting muscular repair post workout. Our pH balanced creatine mix remains stable, so you can drink FAST TWITCH during workouts to keep muscle fibers firing harder, longer. </p>', '', '<p>Do not use if you are pregnant, lactating or nursing, if you are taking medication for high blood pressure, heart, liver, thyroid, psychiatric disease, diabetes, anemia, nervousness, anxiety, depression, glaucoma, seizure disorder, stroke, difficulty in urination or prostate enlargement. Discontinue use and seek a licensed health care professional immediately if chest pain, shortness of breath, dizziness, severe headache, and/or rapid heart beat occurs. Do not use this product if you are using a monoamine oxidase inhibitor (MAOI). Do not use if you have a history of low blood sugar or low blood pressure. Do not use this product while taking sildenafil (Viagra) or if you have migraine headaches. Drink 64 ounces or more of water per day while using this product. Do not consume Vanadyl Sulfate supplements or any supplement containing over 100 mcg of elemental vanadium per daily dosage while using this product. </p>', 'CSPTFAST2.04GRAPPW.html', ''),
(44, 1, 'CytoSport: Fast Twitch Lightning Lemonade 2.04 lb', '<p>CytoSport Fast Twitch is a power workout drink designed to give users explosive strength performance and fuel fast twitch (type II) fibers. It helps to amplify nitric oxide levels for total body pumps to help you protect lean muscle mass. Fast Twitch also has an endo-thermogenic effect which maximizes fat burning after your tough workouts. Awesome muscular explosiveness and size, formerly reserved for the genetically gifted, is the goal of Fast Twitch. Fast Twitch is the first formula specifically designed to fuel fast twitch (type II) muscle fibers for greater muscular power and gains. Fast Twitch helps increase post workout intensity, while enhancing muscular repair. The harder the workout, the more Fast Twitch can help. With Fast Twitch, you may run out of gym time before you run out muscle energy. Fueling type II fibers increases explosive power and speed. This translates into more sets, reps and weight lifted. Plus, type II fibers have greater potential for overall muscle hypertrophy (growth).</p>', '', 0, '<p>CytoCarb II (CytoSport''s unique combination of branching as well as short, medium and long linear chain maltodextrins with very low DE (dextrose equivalence) providing a complex carbohydrate source approximately 96.5% sugar-free), citric acid, natural and artificial flavor, glucuronolactone, malic acid, sodium bicarbonate, acesulfame potassium, xanthan gum, guar gum, carrageenan, turmeric powder (added for color), sucralose.</p>', '<p>Mix one to three scoops/servings of FAST TWITCH in 16 oz of water. Drink 3/4 of bottle 30-45 minutes before workout. Drink remaining 1/4 bottle during workout. Best results are obtained when taken on empty stomach. Take on training days. Drink 64 fl oz of water daily. After workouts, take Muscle Milk within 60 minutes for fast recovery. FAST TWITCH fuels increased explosive strength, while promoting muscular repair post workout. Our pH balanced creatine mix remains stable, so you can drink FAST TWITCH during workouts to keep muscle fibers firing harder, longer.</p>', '', '<p>Do not use if you are pregnant, lactating or nursing, if you are taking medication for high blood pressure, heart, liver, thyroid, psychiatric disease, diabetes, anemia, nervousness, anxiety, depression, glaucoma, seizure disorder, stroke, difficulty in urination or prostate enlargement. Discontinue use and seek a licensed health care professional immediately if chest pain, shortness of breath, dizziness, severe headache, and/or rapid heart beat occurs. Do not use this product if you are using a monoamine oxidase inhibitor (MAOI). Do not use if you have a history of low blood sugar or low blood pressure. Do not use this product while taking sildenafil (Viagra) or if you have migraine headaches. Drink 64 ounces or more of water per day while using this product. Do not consume Vanadyl Sulfate supplements or any supplement containing over 100 mcg of elemental vanadium per daily dosage while using this product.</p>', 'CSPTFAST2.01LEMOPW.html', ''),
(47, 1, 'CytoSport: Muscle Milk Cookies n'' Creme 4.94 lb', '<p>World''s First Evolutionary Muscle Builder. Muscle Milk helps grows muscle faster than gainers, whey or even creatine. This increased muscle growth is possible because Muscle Milk is the first product patterned after Nature''s ultimate anabolic food--human mother''s milk. Locked within human mother''s milk are long-held secrets to muscle growth potential previously considered impossible. After years of study, we''ve unlocked these secrets! We then took the next step and actually duplicated these important muscle growth factors into Muscle Milk.</p>', '', 0, '<p>Evopro (Micellar Alpha and Beta Caseins and Caseinates, Whey Concentrates Rich In Alpha-Lactalbumin, Whey Isolates, Whey Peptides, L-Glutamine, Taurine, Lactoferrin), Leanlipids (Trans Fat Free Lipid Complex Consisting of Sunflower and/or Safflower Oil, MCT''s, Canola Oil, L-Carnitine), Fructose, Maltodextrin, Cytovite I (Vitamin and Mineral Premix consisting of Vitamin A Acetate, Cholecalciferol, D-Alpha-Tocopherol Acetate, Ascorbic Acid, Folate, Thiamine Mononitrate, Riboflavin, Niacinamide, Pyridoxine HCL, Cyanocobalamin, Biotin, Pantothenic Acid, Di-Calcium Phosphate, Potassium Iodide, Potassium Chloride, Ferrous Fumerate, Magnesium Oxide, Copper Gluconate, Zinc Oxide, Chromium Nicotinate), GCC (Proprietary Endogenous Creatine Precursor consisting of Glycocyamine, Betaine Anhydrous), Natural and Artificial Flavors, Acesulfame Potassium, Sucralose, Soy Lecithin</p>', '<p>Mix two scoops in 10-12 fluid ounces of water. Want even more satisfaction? Mix with low-fat milk. Tastes like a real milk shake. Honest!</p>', '<p>This product contains ingredients derived from milk, soy and wheat.</p>', '', 'CSPTMUSC5LBSCNCRPW.html', ''),
(48, 1, 'CytoSport: Muscle Milk Bar Chocolate Peanut Caramel 8 ct', '<p>Whether you need a boost at the gym, in the workplace or anywhere in between, new Muscle Milk Bars deliver all the powerful goodness of Muscle Milk snack for healthy, sustained energy on the go. Just rip one open and power up with satisfying taste and total convenience. Say good-bye to candy-coated empty calories or unproven formulations like some bars on the shelves and go with the proven science and user-approved taste of an energy bar good enough to wear the Muscle Milk name. Muscle Milk bars are strong on protein and balanced with sustained energy ingredients that make for a perfect meal replacement or pre-workout boost. Two delicious flavors to keep your taste buds happy: Chocolate Peanut Caramel and Vanilla Toffee Crunch. </p>\r\n\r\n<p><b><FONT COLOR="FF0000">Due to temperature conditions in the Summer, this product may melt during the shipping process.</FONT></b></p>', '', 0, '<p>MUSCLE MILK PROTEIN BLEND (MILK PROTEIN ISOLATE, WHEY PROTEIN ISOLATE, CALCIUM CASEINATE, MILKPROTEIN CONCENTRATE, SODIUM CASEINATE), HYDROLYZED GELATIN, MALTITOL SYRUP, FRACTIONATED PALM KERNEL OIL, EVAPORATED CANE JUICE, PEANUTS, GLYCERINE, WATER, CORN SYRUP, INVERT SUGAR, NONFAT DRY MILK, PEANUT FLOUR, COCOA POWDER, NATURAL & ARTIFICIAL FLAVORS, POLYDEXTROSE, MALTODEXTRIN, VEGETABLE OILS (PEANUT, CANOLA, COTTONSEED OIL), SALT, HONEY, SOY LECITHIN, CREAM, PARTIALLY HYDROGENATED PALM OIL, MODIFIED POTATO STARCH, SUCRALOSE, DISODIUM PHOSPHATE, CARRAGEENAN GUM, XANTHAN GUM.\r\n</p>', '', '<p>MILK, PEANUTS, SOY. MANUFACTURED IN A FACILITY THAT PROCESSES TREE NUTS. </p>', '', 'CSPTMMBR0008CHOCBR.html', ''),
(49, 1, 'CytoSport: Muscle Milk Bar Vanilla Toffee Crunch 8 ct', '<p>Whether you need a boost at the gym, in the workplace or anywhere in between, new Muscle Milk Bars deliver all the powerful goodness of Muscle Milk snack for healthy, sustained energy on the go. Just rip one open and power up with satisfying taste and total convenience. Say good-bye to candy-coated empty calories or unproven formulations like some bars on the shelves and go with the proven science and user-approved taste of an energy bar good enough to wear the Muscle Milk name. Muscle Milk bars are strong on protein and balanced with sustained energy ingredients that make for a perfect meal replacement or pre-workout boost. Two delicious flavors to keep your taste buds happy: Chocolate Peanut Caramel and Vanilla Toffee Crunch. </p>\r\n\r\n<p><b><FONT COLOR="FF0000">Due to temperature conditions in the Summer, this product may melt during the shipping process.</FONT></b></p>', '', 0, '<p>MUSCLE MILK PROTEIN BLEND (MILK PROTEIN ISOLATE, WHEY PROTEIN ISOLATE, CALCIUM CASEINATE, MILK PROTEIN CONCENTRATE, SODIUM CASEINATE), HYDROLYZED GELATIN, MALTITOL SYRUP, FRACTIONATED PALM KERNEL OIL, EVAPORATED CANE JUICE SOLIDS, PEANUTS, GLYCERINE, WATER, CORN SYRUP, INVERT SUGAR, NONFAT DRY MILK, PEANUT FLOUR, BUTTER, NESTLE BUTTERFINGER, RICE FLOUR, COCOA POWDER, NATURAL & ARTIFICIAL FLAVORS, SALT, POLYDEXTROSE, MALTODEXTRIN, VEGETABLE OILS (COTTONSEED AND/OR SOYBEAN, PALM), MALT EXTRACT, RICE EXTRACT, SUCRALOSE, DISODIUM PHOSPHATE, CARRAGEENAN GUM, DISTILLED MONOGLYCERIDES. NESTLE BUTTERFINGER: CORN SYRUP, GROUND ROASTED PEANUTS, SUGAR, MOLASSES, AND LESS THAN 1% OF CONFECTIONER''S CORN FLAKES,HYDROGENATED PALM KERNEL OIL, SALT, COCOA, SOYBEAN OIL, WHEY, NONFAT MILK, CORNSTARCH, LACTIC ACID ESTERS, SOY LECITHIN, TBHQ AND CITRIC ACID (TO PRESERVE FRESHNESS), ARTIFICIAL FLAVORS, YELLOW 5, RED 40. \r\n</p>', '', '<p>MILK, PEANUTS, SOY. MANUFACTURED IN A FACILITY THAT PROCESSES TREE NUTS. </p>', '', 'CSPTMMBR0008VATOBR.html', ''),
(50, 1, 'CytoSport: Shaker Bottle', '<p>Mix your protein shakes in a cool and convenient shaker bottle.</p>', '', 20, '', '', '', '', '', '');
INSERT INTO `products_description` (`products_id`, `language_id`, `products_name`, `products_description`, `products_url`, `products_viewed`, `other_ingredients`, `directions_of_use`, `allergen_information`, `warnings`, `nutrition_facts_htm`, `nutrition_facts_image`) VALUES
(53, 1, 'Freedom Wholesalers: The Extra Stuff Grape 20 oz 1 ct', '<p>The Stuff is the pinnacle in detoxification technology and convenience. Helping individuals and their families achieve a better standard of living based on their knowledge, skill, and abilities. All our products are formulated to allow you to lower harmful toxin levels, and remove toxins that can build up in your body. Eliminates unwanted toxins for a period of 3-5 hours! The Stuff is fortified with Vitamin B2, and Creatine to aid nutrition replacement. The Stuff does not discriminate against the toxins it removes from your system. The Stuff does not need to be refrigerated. The Stuff comes in a 16oz size for average body size. Included with The Stuff are easy to read instructions and cleansing tips. These cleansing tips are meant to be adhered to as closely as possible in the days prior to the deadline! For the Best results, The Stuff directions must be followed word for word, the day of the deadline. Suggested Use: Shake well and drink entire contents of bottle Wait 15 minutes. Refill the Liquid Stuff bottle with water and drink. Urinate frequently (yellow urine is normal). You may consume one additional 16 oz glass of water if desired. Allow 60 minutes for the Liquid Stuff to become effective. </p>', '', 15, '<p>Deionized filtered water, fructose, fruit pectin, citrix acid, artificial flavor, potassium sorbate, FD & C Red 40 & FD & C Blue 1\r\n</p>', '<p>Consume up to 16 oz of water every two hours until drinking the THE LIQUID STUFF and drink plenty of water afterwards for good health. Shake well and drink entire contents of bottle. Drink plenty of water to help flush your body. Urinate Frequently. Eat light meals including fruit, vegetables and fiber during your cleansing program. </p>', '', '<p>Pregnant or breast-feeding women should consult with their physician before taking this product. </p>', 'FREEEXTR0001GRAPLQ.html', ''),
(54, 1, 'Freedom Wholesalers: The Extra Stuff Fruit Punch 20 oz 1 ct', '<p>The Stuff is the pinnacle in detoxification technology and convenience. Helping individuals and their families achieve a better standard of living based on their knowledge, skill, and abilities. All our products are formulated to allow you to lower harmful toxin levels, and remove toxins that can build up in your body. Eliminates unwanted toxins for a period of 3-5 hours! The Stuff is fortified with Vitamin B2, and Creatine to aid nutrition replacement. The Stuff does not discriminate against the toxins it removes from your system. The Stuff does not need to be refrigerated. The Stuff comes in a 16oz size for average body size. Included with The Stuff are easy to read instructions and cleansing tips. These cleansing tips are meant to be adhered to as closely as possible in the days prior to the deadline! For the Best results, The Stuff directions must be followed word for word, the day of the deadline. Suggested Use: Shake well and drink entire contents of bottle Wait 15 minutes. Refill the Liquid Stuff bottle with water and drink. Urinate frequently (yellow urine is normal). You may consume one additional 16 oz glass of water if desired. Allow 60 minutes for the Liquid Stuff to become effective. </p>', '', 11, '<p>Deionized filtered water, fructose, fruit pectin, citrix acid, artificial flavor, potassium sorbate, FD & C red 40\r\n</p>', '<p>Consume up to 16 oz of water every two hours until drinking the THE LIQUID STUFF and drink plenty of water afterwards for good health. Shake well and drink entire contents of bottle. Drink plenty of water to help flush your body. Urinate Frequently. Eat light meals including fruit, vegetables and fiber during your cleansing program. </p>', '', '<p>Pregnant or breast-feeding women should consult with their physician before taking this product. </p>', 'FREEEXTR0001PUNCLQ.html', ''),
(55, 1, 'Freedom Wholesalers: The Liquid Stuff Grape 16 oz 1 ct', '<p>The Stuff is the pinnacle in detoxification technology and convenience. Helping individuals and their families achieve a better standard of living based on their knowledge, skill, and abilities. All our products are formulated to allow you to lower harmful toxin levels, and remove toxins that can build up in your body. Eliminates unwanted toxins for a period of 3-5 hours! The Stuff is fortified with Vitamin B2, and Creatine to aid nutrition replacement. The Stuff does not discriminate against the toxins it removes from your system. The Stuff does not need to be refrigerated. The Stuff comes in a 16oz size for average body size. Included with The Stuff are easy to read instructions and cleansing tips. These cleansing tips are meant to be adhered to as closely as possible in the days prior to the deadline! For the Best results, The Stuff directions must be followed word for word, the day of the deadline. Suggested Use: Shake well and drink entire contents of bottle Wait 15 minutes. Refill the Liquid Stuff bottle with water and drink. Urinate frequently (yellow urine is normal). You may consume one additional 16 oz glass of water if desired. Allow 60 minutes for the Liquid Stuff to become effective. </p>', '', 7, '<p>Deionized filtered water, fructose, fruit pectin, citrix acid, artificial flavor, potassium sorbate, FD & C Red 40 & FD & C Blue 1\r\n</p>', '<p>Consume up to 16 oz of water every two hours until drinking the THE LIQUID STUFF and drink plenty of water afterwards for good health. Shake well and drink entire contents of bottle. Drink plenty of water to help flush your body. Urinate Frequently. Eat light meals including fruit, vegetables and fiber during your cleansing program. </p>', '', '<p>Pregnant or breast-feeding women should consult with their physician before taking this product. </p>', 'FORTNEUR0020GRPEPK.html', ''),
(56, 1, 'Freedom Wholesalers: The Liquid Stuff Fruit Punch 16 oz 1 ct', '<p>The Stuff is the pinnacle in detoxification technology and convenience. Helping individuals and their families achieve a better standard of living based on their knowledge, skill, and abilities. All our products are formulated to allow you to lower harmful toxin levels, and remove toxins that can build up in your body. Eliminates unwanted toxins for a period of 3-5 hours! The Stuff is fortified with Vitamin B2, and Creatine to aid nutrition replacement. The Stuff does not discriminate against the toxins it removes from your system. The Stuff does not need to be refrigerated. The Stuff comes in a 16oz size for average body size. Included with The Stuff are easy to read instructions and cleansing tips. These cleansing tips are meant to be adhered to as closely as possible in the days prior to the deadline! For the Best results, The Stuff directions must be followed word for word, the day of the deadline. Suggested Use: Shake well and drink entire contents of bottle Wait 15 minutes. Refill the Liquid Stuff bottle with water and drink. Urinate frequently (yellow urine is normal). You may consume one additional 16 oz glass of water if desired. Allow 60 minutes for the Liquid Stuff to become effective. </p>', '', 10, '<p>Deionized filtered water, fructose, fruit pectin, citrix acid, artificial flavor, potassium sorbate, FD & C red 40\r\n</p>', '<p>Consume up to 16 oz of water every two hours until drinking the THE LIQUID STUFF and drink plenty of water afterwards for good health. Shake well and drink entire contents of bottle. Drink plenty of water to help flush your body. Urinate Frequently. Eat light meals including fruit, vegetables and fiber during your cleansing program. </p>', '', '<p>Pregnant or breast-feeding women should consult with their physician before taking this product. </p>', 'FORTVITA0020FRTPPK.html', ''),
(66, 1, 'ISS: Oh Yeah Chocolate & Caramel Bar 12ct', '<p>Oh Yeah! Good Grab</p>\r\n<p>One bite and you''ll understand how the bar got its name. Oh Yeah! It''s that good!</p>\r\n<ul>\r\n  <li>Sustained Energy </li>\r\n  <li>High Protein</li>\r\n</ul>\r\n<p>The Good Grab&#8482; symbol is an easy way to quickly identify OhYeah!&reg; nutritional products made from healthy, Wholesome ingredients that offer premium taste and hunger satisfaction, yet are reduced in calories, lower in fat, saturated fat, sodium or sugar. To learn more about Good Grab&#8482; go to www.ohyeahnutrition.com/goodgrab.</p>\r\n<p><b><FONT COLOR="FF0000">Due to temperature conditions in the Summer, this product may melt during the shipping process.</FONT></b></p>', '', 1, '<p>Peanuts, protein blend [(OhYeah! Blend consisting  of whey protein isolate, soy protein isolate, milk protein isolate, milk protein concentrate, calcium caseinate), hydrolyzed gelatin], chocolate coating [chocolate liquor, maltitol, cocoa butter, sodium caseinate, milk fat, soy lecithin (an emulsifier), sucralose, vanillin (an artificial flavor), vanilla and salt], caramel (sugar, corn syrup, non fat dry milk, maltitol syrup, cocoa butter, corn starch, butter, sucralose, soy lecithin, natural and artificial flavor), soy nuts, glycerine, peanut flour, fractionated palm kernel oil, lecithin (an emulsifier), natural and artificial flavors, salt, sucralose, and potassium sorbate added as a preservative. </p>', '<p>Peel off the stay-fresh wrapper and indulge in sweet, gooey goodness. </p>', '<p>This product contains peanuts, milk and soy and is produced in a facility that processes soy, dairy, egg, and wheat ingredients, preanuts and tree nuts. </p>', '', 'ISSOHYE0012CHOCBR.HTML', ''),
(67, 1, 'ISS: Oh Yeah Peanut Butter Strawberry Bar 12ct', '<p>No pain, no gain should define your training - not your protein bar! So reward your workout with the gooey, sweet taste of OhYeah! Containing 26-28 grams of muscle-building protein, heart-healthy fats, low sugar and absolutely no trans fats in a bar that''s nothing short of amazing. More than just another flavorless brick, OhYeah! looks and eats like a real candy bar that''s so good you''ll know how it got its'' name. OhYeah! - It''s that good!</p>\r\n\r\n<p><b><FONT COLOR="FF0000">Due to temperature conditions in the Summer, this product may melt during the shipping process.</FONT></b></p>', '', 0, '<p>Soy nuts, protein blend [(OhYeah! Blend consisting  of whey protein isolate, soy protein isolate, milk protein isolate, milk protein concentrate, calcium caseinate), hydrolyzed gelatin], peanut butter coating [maltitol, fractionated palm kernel oil, milk protein isolate, partially defatted peanut flour, whey, peanut butter (peanuts, peanut oil, dextrose, partially hydrogenated vegetable oil [rapeseed & cottonseed oil], salt), soy lecithin and sucralose], strawberry jelly spread [corn syrup, fractionated palm kernel oil, dehydrated apples (contains sulfites), glycerine, natural flavors, corn starch, pectin, citric acid, soy lecithin, red #40, yellow #6 and blue #1], peanuts, glycerine, fractionated palm kernel oil, lecithin (an emulsifier), natural and artificial flavors, salt, sucralose, and potassium sorbate added as a preservative.</p>', '<p>Peel off the stay-fresh wrapper and indulge in sweet, gooey goodness.</p>', '<p>This product contains peanuts, milk and soy and is produced in a facility that processes soy, dairy, egg, and wheat ingredients, peanuts and tree nuts.</p>', '', 'ISSOHYE0012PBSTBR.html', ''),
(74, 1, 'Labrada: Lean Body Chocolate 42ct', '<p>Lean Body Meal Replacement contains 45 grams of LeanPro, our exclusive blend of highest quality proteins, including ion-exchanged whey protein to provide support for your muscle growth! Smooth, delicious taste! Mixes instantly without a blender! Low sugar, Low fat, and Low sodium! No hydrogenated oil, salt, corn syrup solids, fructose or mono and diglycerides. Packed with all of your essential vitamins and minerals! </p>', '', 0, '<p>LeanPro (Cross Flow Micro Ultrafiltered Whey Protein Concentrate, Soy Protein Isolate, Cross Flow Micro Ultrafiltered Whey Protein Isolate, Milk Protein Isolate, Glutamine Peptides), EFA Plex (Sunflower Oil, Creamer [High Oleic Sunflower Oil, Corn Syrup Solids, Sodium Caseinate, Mono & Diglycerides, Dipotassium Phosphate, Tricalcium Phosphate, Soy Lecithin, Tocopherols], MCT Oil, Flaxseed Oil, borage Oil), Coca (Processed with Alkali), Fiber Plex (Polydextrose, Fibersol 2 Fiber, Cellulose Gum, Oat Flour, Rice Bran), Natural and Artificial Flavors, Vitamin & Mineral Blend (Di-Calcium Phosphate, Magnesium Oxide, Ascorbic Acid, Vitamin E Acetate, Niacinamide, Electrolyctic Iron, Zinc Oxide, D-Caclium Pantothenate, Pyridoxine Hydrochloride, Copper Gluconate, Riboflavin, Vitamin A Acetate, Thaimin Mononitrate, Folic Acid, Potassium Iodide, Cyanocobalamin), Fructose, Salt, Sucralose, Acesulfame Potassium\r\n</p>', '', '', '', 'LABRLNBY0042CHOCPK.html', ''),
(75, 1, 'Labrada: Lean Body Vanilla 42ct', '<p>Lean Body Meal Replacement contains 45 grams of LeanPro, our exclusive blend of highest quality proteins, including ion-exchanged whey protein to provide support for your muscle growth! Smooth, delicious taste! Mixes instantly without a blender! Low sugar, Low fat, and Low sodium! No hydrogenated oil, salt, corn syrup solids, fructose or mono and diglycerides. Packed with all of your essential vitamins and minerals! </p>', '', 0, '<p>LeanPro (Cross Flow Micro Ultrafiltered Whey Protein Concentrate, Soy Protein Isolate, Cross Flow Micro Ultrafiltered Whey Protein Isolate, Milk Protein Isolate, Glutamine Peptides), EFA Plex (Sunflower Oil, Creamer [High Oleic Sunflower Oil, Corn Syrup Solids, Sodium Caseinate, Mono & Diglycerides, Dipotassium Phosphate, Tricalcium Phosphate, Soy Lecithin, Tocopherols], MCT Oil, Flaxseed Oil, borage Oil), Coca (Processed with Alkali), Fiber Plex (Polydextrose, Fibersol 2 Fiber, Cellulose Gum, Oat Flour, Rice Bran), Natural and Artificial Flavors, Vitamin & Mineral Blend (Di-Calcium Phosphate, Magnesium Oxide, Ascorbic Acid, Vitamin E Acetate, Niacinamide, Electrolyctic Iron, Zinc Oxide, D-Caclium Pantothenate, Pyridoxine Hydrochloride, Copper Gluconate, Riboflavin, Vitamin A Acetate, Thaimin Mononitrate, Folic Acid, Potassium Iodide, Cyanocobalamin), Fructose, Salt, Sucralose, Acesulfame Potassium\r\n</p>', '', '', '', 'LABRLNBY0042VANIPK.html', ''),
(76, 1, 'Labrada: Lean Body Mass 60 Chocolate 6 lb', '<p>Lean Body Mass 60 is designed for and by hard gainers. If you need help putting on muscle and weight, then Lean Body Mass 60 is for you. We''re so sure you''ll pack on genuine muscle mass with this muscle-builder shake, we''ll guarantee your money back if you don''t. Lean Body Mass 60 is easy to use. Just eat your normal breakfast, lunch and dinner. Drink a delicious Lean Body Mass 60 shake at mid-morning, mid-afternoon, and again before bedtime to get the best results. Lean Body Mass 60 contains a time-released blend of 6 high-quality proteins plus creatine monohydrate to support anabolic muscle growth. Each serving of Lean Body Mass 60 provides 60 grams of protein - the highest protein content of any product in its class. In addition, you will experience no bloating with Lean Body Mass 60, because it''s 98% lactose free, 96% fat free, and contains no aspartame, sucrose or fructose. And just like every Labrada'' product, Lean Body Mass 60 is lab tested to meet the label claim of potency. </p>', '', 0, '<p>Lean Pro Protein Blend (Whey Protein Concentrate, Hydrolyzed Whey Protein Isolate, Milk Protein Isolate, Calcium Caseinate, Glutamine Peptides), Low D.E. Maltodextrin, Cocoa (Processed with Alkali), Natural & Artificial Flavor, LBM60 Vitamin & Mineral Blend (Di-Calcium Phosphate, Magnesium Oxide, Ascorbic Acid, Vitamin E Acetate, Niacinamide, Electrolytic Iron, Zinc Oxide, D-Calcium Pantothenate, Pyridoxine Hydrochloride, Copper Gluconate, Riboflavin, Vitamin A Acetate, Thiamin Mononitrate, Folic Acid, Potassium Iodide, Cyanocobalamin), Cellulose Gum, Acesulfame Potassium (Ace-K), Sucralose\r\n</p>', '', '', '', 'LABRMASS06LBCHOCPW.html', ''),
(77, 1, 'Labrada: Lean Body Mass 60 Vanilla 6 lb', '<p>Lean Body Mass 60 is designed for and by hard gainers. If you need help putting on muscle and weight, then Lean Body Mass 60 is for you. We''re so sure you''ll pack on genuine muscle mass with this muscle-builder shake, we''ll guarantee your money back if you don''t. Lean Body Mass 60 is easy to use. Just eat your normal breakfast, lunch and dinner. Drink a delicious Lean Body Mass 60 shake at mid-morning, mid-afternoon, and again before bedtime to get the best results. Lean Body Mass 60 contains a time-released blend of 6 high-quality proteins plus creatine monohydrate to support anabolic muscle growth. Each serving of Lean Body Mass 60 provides 60 grams of protein - the highest protein content of any product in its class. In addition, you will experience no bloating with Lean Body Mass 60, because it''s 98% lactose free, 96% fat free, and contains no aspartame, sucrose or fructose. And just like every Labrada'' product, Lean Body Mass 60 is lab tested to meet the label claim of potency. </p>', '', 0, '<p>Lean Pro Protein Blend (Whey Protein Concentrate, Hydrolyzed Whey Protein Isolate, Milk Protein Isolate, Calcium Caseinate, Glutamine Peptides), Low D.E. Maltodextrin, Crealean, Natural & Artificial Flavor, LBM60 Vitamin & Mineral Blend (Di-Calcium Phosphate, Magnesium Oxide, Ascorbic Acid, Vitamin E Acetate, Niacinamide, Electrolytic Iron, Zinc Oxide, D-Calcium Pantothenate, Pyridoxine Hydrochloride, Copper Gluconate, Riboflavin, Vitamin A Acetate, Thiamin Mononitrate, Folic Acid, Potassium Iodide, Cyanocobalamin), Cellulose Gum, Acesulfame Potassium (Ace-K), Sucralose\r\n</p>', '', '', '', 'LABRMASS06LBVANIPW.html', ''),
(78, 1, 'Labrada: Shaker Bottle', '<p>Mix your protein shakes in a cool and convenient shaker bottle. </p>', '', 13, '', '', '', '', '', ''),
(80, 1, 'LG Sciences: Formadrol Extreme 90ct', '<p>Formadrol Extreme combines two of the best-known anti-estrogen ingredients to make one killer product whether you are on cycle or off cycle. This has one great quality in the body! It is known as a suicide inhibitor. This product is proven to bind to Aromatase, which is the nasty enzyme that converts Testosterone into Estrogen. Estrogen conversion through Aromatase is one key factor in destroying your gains and stopping you from being lean and vascular. On cycle, it is a great way to prevent the symptoms of extra estrogen like increased water weight and more. Off cycle it provides a positive Testosterone to Estrogen ratio that can help restore natural Testosterone production. Additionally we added Diadzin, which is a phyto-chemical that acts in two great ways in the body. One, it is a competitive estrogen inhibitor, which prevents binding of real estrogen to your receptors. Two, it is shown in the literature to be a partial binder of 5 alpha reductase (5aR). This is the other nasty enzyme that is responsible for hair loss! Post cycle it may help re-grow lost hair and boost normal testosterone levels. Used on or off cycle to increase androgen levels and reduce estrogen. Building a few pounds of muscle simply requires that you lift heavy and eat a surplus of calories. Never has man been satisfied with just a litte, though. That''s why Legal Gear brings you the only products that are sure to put mass on you like no other. In light of the recent ban, this is the next generation of muscle building supplements. WARNING: This product contains chemicals known to the State of California to cause birth defects or other reproductive harm. </p>', '', 17, '<p>Stearic acid, magnesium stearate, gelatin, FD&C red dye #40, FD&C blue dye #1, titanium dioxide\r\n</p>', '<p>As a dietary supplement take 2-4 capsules per day divided in the morning and at night. For best results use for 4 to 8 weeks. Do not exceed 8 weeks of continuous use. Always check with a physician before using any dietary supplement. </p>', '', '<p>KEEP OUT OF REACH OF CHILDREN. Consult your physician before using this dietary supplement. Do not use this product if you are at risk or are being treated for high blood pressure, heart, kidney, thyroid, or psychiatric disease, difficulty in urinating, prostate enlargement, anxiety, depression, seixure disorder, or stroke. Consult your healthcare professional before use if you are taking antidepressants, MAO inhibitors, or any other prescription drugs. Discontinue use and consult your healthcare professional if dizziness, sleeplessness, tremors, nervousness, headache or heart palpitations occur. Consult your healthcare practitioner before beginning any weight loss program. </p>', 'LEGAFORM00600000CP.html', ''),
(82, 1, 'LG Sciences: Methyl 1-D 90ct', '<p>"Everyone wants to be the "Alpha Male", the guy who gets all the women, who other men look up to and the guy that is successful and popular. The hidden secret for the success of these "alpha males" isn''t looks or brains, the secret is a cascade of hormones that effect everything about that man, his stamina, sexual performance, you know, his edge. This cascade of hormones has been extensively studied, but until now, the way to boost this hormonal cascade was lost to us ordinary men. Enter Methyl 1-D the ultimate anabolic hormonal optimizer. These "alpha male" hormones, are dependant on their availability in the bloodstream and having the raw materials available to make them."</p>', '', 1, '<p>Amidosarcosine Ester, DiCalcium Phosphate Dihidrate, Magnesium Stearate, Gelatin, FD&C Red Dye #40, FD&C Blue Dye #1, Titanium Dioxide\r\n</p>', '<p>As a dietary supplement take 4 to 6 capsules per day in two divided doses. Use Methyl 1-D for 4 to 6 week cycles. As Post Cycle Therapy use Formadrol extreme as directed to maintain your gains and boost natural testosterone levels. </p>', '', '<p>KEEP OUT OF THE REACH OF CHILDREN. Consult your physician before using this dietary supplement. Do not use this product if you are at risk or are being treated for high blood pressure, heart, kidney, thyroid, or psychiatric disease, difficulty in urinating, prostate enlargement, anxiety, depression, seizure disorder, or stroke, Consult your healthcare professional before use if you are taking any prescription drugs. Consult your healthcare practitioner before beginning any weight loss program. </p>', 'LEGAMET100750000CP.html', ''),
(85, 1, 'Met-Rx: Big 100 Colossal Cookie Crunch Bar 12ct', '<p>The Big 100 is Power Anytime! A meal replacement bar with our exclusive METAMYOSYN protein blend, this power-packed bar is a great tasting way to fuel up on demand! </p>\r\n\r\n<p><b><FONT COLOR="FF0000">Due to temperature conditions in the Summer, this product may melt during the shipping process.</FONT></b></p>', '', 0, '<p>Cocoa flavored soy crisp (soy protein isolate, rice starch, natural and artificial flavor), vanilla creme topping (high fructose corn syrup, corn syrup, milk protein isolate, fractionated palm kernel oil, soy lecithin, natural flavors), corn syrup, chocolate flavored coating (sugar, palm kernel and palm oils, cocoa (processed with alkali), nonfat milk, sorbitan monostearate, soy lecithin, vanilla), chocolate chunks (sugar, choclate liquor, cocoa butter, soy lecithin, artificial flavor, salt, vanilla), cocoa powder (processed with alkali), Metamyosyn VPX Protein Blend (milk protein isolate, whey protein concentrate, whey protein isolate, egg white, L-glutamine), peanut flour, glycerin, water, choclate chunks (sugar, chocolate liquor, cocoa butter, soy lecithin, artificial flavor, salt, vanilla), roasted peanuts, dextrose, natural flavor, vitamin and mineral blend (ascorbic acid, tricalcium phosphate, d-alpha tocopheryl acetate, niacinamide, zinc oxide, copper gluconate, calcium d-pantothenate, vitamin a palmitate, pyridoxine hydrocholride, thiamin mononitrate, riboflavin, folic acid, biotin, potassium iodide, cyanocobalamin), dipotassium phosphate, xanthan gum, sucralose, soy lecithin, almond butter.</p>', '<p>Adults, for best results drink 8fl. oz. of water or other liquid with this bar.</p>', '<p>This product contains milk, egg, soy and peanut ingredients. Produced in a facility that also processes wheat, tree nut and fish products.</p>', '<p>This product contains sugar alcohols, which my cause gastrointestinal discomfort. Excessive consumption may have a laxative effect.</p>', 'METXMEBC0012COOKBR.html', ''),
(86, 1, 'Met-Rx: Big 100 Colossal Crisp Apple Pie Bar 12ct', '<p>The Big 100 is Power Anytime! A meal replacement bar with our exclusive METAMYOSYN protein blend, this power-packed bar is a great tasting way to fuel up on demand! </p>\r\n\r\n<p><b><FONT COLOR="FF0000">Due to temperature conditions in the Summer, this product may melt during the shipping process.</FONT></b></p>', '', 0, '<p>Soy crisps (soy protein isolate, tapioca starch, salt), apple layer (Fruit (apple puree concentrate, deionized apple juice concentrate), sugar, corn syrup, glycerol, coconut oil, corn starch, wheat fiber, malic acid, calcium lactate, natural flavor, sunflower lecithin), yogurt coating (sugar, palm kernel oil, nonfat dry milk, yogurt powder (whey protein concentrate, skim milk, yogurt culture), soy lecithin, salt, natural flavor), Metamyosyn VPX Protein Blend (milk protein isolate, whey protein concentrate, whey protein isolate, egg white, L-glutamine), maltitol syrup, dried apple, palm kernel oil, glycerin oil, water, canola oil, ground cinnamon, natural flavor, vitamin and mineral blend (ascorbic acid, tricalcium phosphate, d-alpha tocopheryl acetate, niacinamide, zinc oxide, copper gluconate, calcium d-pantothenate, vitamin a palmitate, pyridoxine hydrocholride, thiamin mononitrate, riboflavin, folic acid, biotin, potassium iodide, cyanocobalamin), xanthan gum, dipotassium phosphate, salt, sucralose, soy lecithin. </p>', '<p>Adults, for best results drink 8fl. Ounces of water or other liquid with this bar. </p>', '<p>This product contains milk, egg, soy and peanut ingredients. Produced in a facility that also processes wheat, tree nut and fish products.</p>', '<p>This product contains milk, egg, soy and peanut ingredients. Produced in a facility that also processes wheat, tree nut and fish products. </p>', 'METXMEBC0012APPLBR.html', ''),
(87, 1, 'Met-Rx: Big 100 Colossal Peanut Caramel Bar 12ct', '<p>The Big 100 is Power Anytime! A meal replacement bar with our exclusive METAMYOSYN protein blend, this power-packed bar is a great tasting way to fuel up on demand! </p>\r\n\r\n<p><b><FONT COLOR="FF0000">Due to temperature conditions in the Summer, this product may melt during the shipping process.</FONT></b></p>', '', 0, '', '<p>Adults, for best results drink 8 fl. ounces of water or other liquid with this bar. </p>', '<p>Contains Milk, Wheat, Peanuts, Egg And Soy Ingredients. </p>', '<p>Warnings: This product contains sugar alcohols, which my cause gastrointestinal discomfort. Excessive consumption may have a laxative effect. </p>', 'METXMEBC0012PEANBR.html', ''),
(88, 1, 'MHP: A-Bomb 224 tablets', '<p>A-BOMB is, without question, the most exciting development in the science of muscle growth. It is the first supplement to trigger your genetic muscle building machinery from multiple pathways and synergistic anabolic actions. A-BOMB''s highly researched ingredients work at the DNA level through non-hormonal signal transduction by manipulating key muscle growth regulators mTOR, PKB, p70s6k, 4E-BP1, FOXO and MAFbx. A-BOMB maximizes this process through two critical pathways. The first pathway is the  &#8224;˜nutrient dependent pathway'', in which A-BOMB uses specific amino acids and nutrients to modulate signaling through pathways traditionally triggered by hormones. The second pathway is the  &#8224;˜insulin dependent pathway'', in which A-BOMB has the unique ability to mediate the release of insulin without raising blood sugar. Simultaneously signaling these pathways allows for maximum gene activation at the DNA level to  &#8224;˜turn on'' the muscle building process. A-BOMB even goes a step further to improve muscle gene activation by upregulating your DNA''s central control system. Using a clinically proven DNA remodeling compound, A-BOMB removes damaged cells and repairs DNA damage allowing your genetic cellular machinery to respond better to the signaling of these key muscle growth regulators. In addition to its critical role at the genetic level, A-BOMB''s unique ability to augment a controlled, steady release of insulin creates the ideal insulinotropic anabolic environment, increasing protein synthesis to stimulate muscle growth. A-BOMB also induces muscle hypertrophy through a very powerful anabolic mechanism called Intracellular Osmolarity (IO), which creates an anabolic reaction within the muscle cell. Interestingly, Intracellular Osmolarity works both independently as well as synergistically to mTOR signal transduction. So, not only is Intracellular Osmolarity anabolic in itself, it will further enhance gene expression.\r\n</p>', '', 0, '<p>Microcrystalline cellulose, guar gum, polyvinylpyrrolidone, hydroxypropylcellulose, vegetable stearin, anhydrous dicalcium phosphate, stearic acid, magnesium stearate, silica and film coating (polyvinyl alcohol, polyethylene glycol, talc, titanium dioxide). </p>', '<p>As a dietary supplement, take 4 tablets in the morning and 4 tablets in the evening. </p>', '', '', 'MAXHABOM02240000TB.html', ''),
(89, 1, 'MHP: Cyclin-GF 120ct', '<p>Until now you''ve only been tapping into a fraction of your muscle building potential, because within your body are millions of inactive Muscle Precursor Cells (MPCs). Imagine the impact that awakening these millions of MPCs and forcing them to develop into mature muscle fibers will have on your muscle growth and strength gains. Through the development of Cyclin-GF, MHP''s research team has discovered the secret of how to activate these dormant but precious Muscle Precursor Cells (MPCs) and promote the anabolic internal environment necessary to stimulate Muscle Precursor Cell (MPC) activation and carry them through the vital full phase muscle growth cycle needed to achieve massive muscle fiber maturity. Never before has any supplement or even pharmaceutical compound been able to activate dormant precursor muscle cells and carry them through the full phase muscle growth cycle to generate enormous new gains in muscle mass. Cyclin-GF is your key to unlocking this muscle building phenomenon. It''s time to wake up the millions of sleeping muscle precursor cells and stimulate them to grow into mature full blown massive muscles. Be one of the first to experience the latest breakthrough in the science of muscle building. </p>', '', 1, '<p>Gelatin, microcrystalline cellulose, magnesium stearate, silica, FD&C blue #1, FD&C red #3, and titanium dioxide\r\n</p>', '<p>As a dietary supplement, take 4 caplets with water before bed. </p>', '', '<p>Consult with your physician before using this product. Do not take this product if you have a medical condition indlucing, but not limited to, diabetes, heart disease, psychological disorder, prostate hypertrophy or hormonal abnormalities. Consult with your physician if you are taking antidepressants, MAO inhibitors or other prescription drugs. This product should not be used by women or males under the age of 18. </p>', 'MAXHCYCL01200000CP.html', ''),
(93, 1, 'MHP: Probolic-SR Chocolate 2 lb', '<p>"The patented Micro-Feed Technology is the biggest breakthrough ever in protein supplementation. This pharmaceutical delivery system is designed to provide a continual supply of amino acids for up to 12 hours! The Micro-Feed Technology works like an IV drip continually supplying a "metered dose" of high concentrations of Critical Five Amino Acids so that you always stay in an extremely anabolic muscle building state and never slip into a catabolic state. This technology also increases bio-availability making full 100% utilization of every gram of protein. "No Wasted Protein-No Wasted Muscle!"</p>', '', 0, '<p>Probolic Sustained Release Amino Acid Enhanced Protein matrix {Supro soy protein isolate (with lecithin), calcium caseinate (milk derivative), and whey (milk) protein concentrate, Branched Chain Amino Acid Blend (L-Isoleucine, L-Leucine and L-Valine)], cocoa,natural & artificial flavors, Lipobolic Advanced Lipid Complex [medium chain triglycerides complex, borage seed oil powder (10% GLA), conjugated linoleic acid complex (78% CLA), evening primrose seed oil (4.8% GLA), flaxseed powder (8% ALA), and omega III complex (fish oil) (7.5% EPA & DHA)], stearic acid, gum blend (cellulose gum, xanthan gum, and carrageenan), salt, silica, acesulfame potassium, sucralose. \r\n</p>', '<p>Mix 1 to 2 scoops of Probolic-SR with 8 to 16 oz. of water in a shaker bottle or blender. Consume 2-3 servings a day or use as recommended by a physician or licensed nutritionist. </p>', '', '', 'MAXHPROB02LBCHOCPW.html', '');

-- --------------------------------------------------------

--
-- Table structure for table `products_to_categories`
--

CREATE TABLE IF NOT EXISTS `products_to_categories` (
  `products_id` int(11) NOT NULL DEFAULT '0',
  `categories_id` int(11) NOT NULL DEFAULT '0'
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

--
-- Dumping data for table `products_to_categories`
--

INSERT INTO `products_to_categories` (`products_id`, `categories_id`) VALUES
(0, 30),
(0, 107),
(9, 8),
(10, 8),
(16, 13),
(17, 13),
(18, 13),
(19, 13),
(20, 13),
(22, 14),
(23, 14),
(24, 14),
(25, 14),
(37, 33),
(39, 33),
(41, 8),
(42, 3),
(43, 3),
(44, 3),
(47, 8),
(48, 14),
(49, 14),
(50, 12),
(53, 19),
(54, 19),
(55, 19),
(56, 19),
(66, 14),
(67, 14),
(74, 13),
(75, 13),
(76, 16),
(77, 16),
(78, 12),
(80, 2),
(82, 2),
(85, 14),
(86, 14),
(87, 14),
(88, 2),
(89, 2),
(93, 8);

-- --------------------------------------------------------

--
-- Table structure for table `zones`
--

CREATE TABLE IF NOT EXISTS `zones` (
  `zone_id` int(11) NOT NULL,
  `zone_country_id` int(11) NOT NULL DEFAULT '0',
  `zone_code` varchar(32) NOT NULL,
  `zone_name` varchar(32) NOT NULL
) ENGINE=MyISAM AUTO_INCREMENT=66 DEFAULT CHARSET=latin1;

--
-- Dumping data for table `zones`
--

INSERT INTO `zones` (`zone_id`, `zone_country_id`, `zone_code`, `zone_name`) VALUES
(1, 223, 'AL', 'Alabama'),
(2, 223, 'AK', 'Alaska'),
(3, 223, 'AS', 'American Samoa'),
(4, 223, 'AZ', 'Arizona'),
(5, 223, 'AR', 'Arkansas'),
(6, 223, 'AF', 'Armed Forces Africa'),
(7, 223, 'AA', 'Armed Forces Americas'),
(8, 223, 'AC', 'Armed Forces Canada'),
(9, 223, 'AE', 'Armed Forces Europe'),
(10, 223, 'AM', 'Armed Forces Middle East'),
(11, 223, 'AP', 'Armed Forces Pacific'),
(12, 223, 'CA', 'California'),
(13, 223, 'CO', 'Colorado'),
(14, 223, 'CT', 'Connecticut'),
(15, 223, 'DE', 'Delaware'),
(16, 223, 'DC', 'District of Columbia'),
(17, 223, 'FM', 'Federated States Of Micronesia'),
(18, 223, 'FL', 'Florida'),
(19, 223, 'GA', 'Georgia'),
(20, 223, 'GU', 'Guam'),
(21, 223, 'HI', 'Hawaii'),
(22, 223, 'ID', 'Idaho'),
(23, 223, 'IL', 'Illinois'),
(24, 223, 'IN', 'Indiana'),
(25, 223, 'IA', 'Iowa'),
(26, 223, 'KS', 'Kansas'),
(27, 223, 'KY', 'Kentucky'),
(28, 223, 'LA', 'Louisiana'),
(29, 223, 'ME', 'Maine'),
(30, 223, 'MH', 'Marshall Islands'),
(31, 223, 'MD', 'Maryland'),
(32, 223, 'MA', 'Massachusetts'),
(33, 223, 'MI', 'Michigan'),
(34, 223, 'MN', 'Minnesota'),
(35, 223, 'MS', 'Mississippi'),
(36, 223, 'MO', 'Missouri'),
(37, 223, 'MT', 'Montana'),
(38, 223, 'NE', 'Nebraska'),
(39, 223, 'NV', 'Nevada'),
(40, 223, 'NH', 'New Hampshire'),
(41, 223, 'NJ', 'New Jersey'),
(42, 223, 'NM', 'New Mexico'),
(43, 223, 'NY', 'New York'),
(44, 223, 'NC', 'North Carolina'),
(45, 223, 'ND', 'North Dakota'),
(46, 223, 'MP', 'Northern Mariana Islands'),
(47, 223, 'OH', 'Ohio'),
(48, 223, 'OK', 'Oklahoma'),
(49, 223, 'OR', 'Oregon'),
(50, 223, 'PW', 'Palau'),
(51, 223, 'PA', 'Pennsylvania'),
(52, 223, 'PR', 'Puerto Rico'),
(53, 223, 'RI', 'Rhode Island'),
(54, 223, 'SC', 'South Carolina'),
(55, 223, 'SD', 'South Dakota'),
(56, 223, 'TN', 'Tennessee'),
(57, 223, 'TX', 'Texas'),
(58, 223, 'UT', 'Utah'),
(59, 223, 'VT', 'Vermont'),
(60, 223, 'VI', 'Virgin Islands'),
(61, 223, 'VA', 'Virginia'),
(62, 223, 'WA', 'Washington'),
(63, 223, 'WV', 'West Virginia'),
(64, 223, 'WI', 'Wisconsin'),
(65, 223, 'WY', 'Wyoming');

--
-- Indexes for dumped tables
--

--
-- Indexes for table `address_book`
--
ALTER TABLE `address_book`
  ADD PRIMARY KEY (`address_book_id`),
  ADD KEY `idx_address_book_customers_id` (`customers_id`);

--
-- Indexes for table `categories`
--
ALTER TABLE `categories`
  ADD PRIMARY KEY (`categories_id`),
  ADD KEY `idx_categories_parent_id` (`parent_id`);

--
-- Indexes for table `categories_description`
--
ALTER TABLE `categories_description`
  ADD PRIMARY KEY (`categories_id`,`language_id`),
  ADD KEY `idx_categories_name` (`categories_name`);

--
-- Indexes for table `countries`
--
ALTER TABLE `countries`
  ADD PRIMARY KEY (`countries_id`),
  ADD KEY `IDX_COUNTRIES_NAME` (`countries_name`);

--
-- Indexes for table `currencies`
--
ALTER TABLE `currencies`
  ADD PRIMARY KEY (`currencies_id`);

--
-- Indexes for table `customers`
--
ALTER TABLE `customers`
  ADD PRIMARY KEY (`customers_id`);

--
-- Indexes for table `customers_info`
--
ALTER TABLE `customers_info`
  ADD PRIMARY KEY (`customers_info_id`);

--
-- Indexes for table `languages`
--
ALTER TABLE `languages`
  ADD PRIMARY KEY (`languages_id`),
  ADD KEY `IDX_LANGUAGES_NAME` (`name`);

--
-- Indexes for table `orders`
--
ALTER TABLE `orders`
  ADD PRIMARY KEY (`orders_id`);

--
-- Indexes for table `orders_products`
--
ALTER TABLE `orders_products`
  ADD PRIMARY KEY (`orders_products_id`);

--
-- Indexes for table `orders_total`
--
ALTER TABLE `orders_total`
  ADD PRIMARY KEY (`orders_total_id`),
  ADD KEY `idx_orders_total_orders_id` (`orders_id`);

--
-- Indexes for table `products`
--
ALTER TABLE `products`
  ADD PRIMARY KEY (`products_id`),
  ADD UNIQUE KEY `lsfs_item_id` (`lsfs_item_id`),
  ADD KEY `idx_products_date_added` (`products_date_added`);

--
-- Indexes for table `products_description`
--
ALTER TABLE `products_description`
  ADD PRIMARY KEY (`products_id`,`language_id`),
  ADD KEY `products_name` (`products_name`);

--
-- Indexes for table `products_to_categories`
--
ALTER TABLE `products_to_categories`
  ADD PRIMARY KEY (`products_id`,`categories_id`);

--
-- Indexes for table `zones`
--
ALTER TABLE `zones`
  ADD PRIMARY KEY (`zone_id`);

--
-- AUTO_INCREMENT for dumped tables
--

--
-- AUTO_INCREMENT for table `address_book`
--
ALTER TABLE `address_book`
  MODIFY `address_book_id` int(11) NOT NULL AUTO_INCREMENT,AUTO_INCREMENT=7;
--
-- AUTO_INCREMENT for table `categories`
--
ALTER TABLE `categories`
  MODIFY `categories_id` int(11) NOT NULL AUTO_INCREMENT,AUTO_INCREMENT=122;
--
-- AUTO_INCREMENT for table `countries`
--
ALTER TABLE `countries`
  MODIFY `countries_id` int(11) NOT NULL AUTO_INCREMENT,AUTO_INCREMENT=224;
--
-- AUTO_INCREMENT for table `currencies`
--
ALTER TABLE `currencies`
  MODIFY `currencies_id` int(11) NOT NULL AUTO_INCREMENT,AUTO_INCREMENT=3;
--
-- AUTO_INCREMENT for table `customers`
--
ALTER TABLE `customers`
  MODIFY `customers_id` int(11) NOT NULL AUTO_INCREMENT,AUTO_INCREMENT=6;
--
-- AUTO_INCREMENT for table `languages`
--
ALTER TABLE `languages`
  MODIFY `languages_id` int(11) NOT NULL AUTO_INCREMENT,AUTO_INCREMENT=2;
--
-- AUTO_INCREMENT for table `orders`
--
ALTER TABLE `orders`
  MODIFY `orders_id` int(11) NOT NULL AUTO_INCREMENT,AUTO_INCREMENT=9;
--
-- AUTO_INCREMENT for table `orders_products`
--
ALTER TABLE `orders_products`
  MODIFY `orders_products_id` int(11) NOT NULL AUTO_INCREMENT,AUTO_INCREMENT=11;
--
-- AUTO_INCREMENT for table `orders_total`
--
ALTER TABLE `orders_total`
  MODIFY `orders_total_id` int(10) unsigned NOT NULL AUTO_INCREMENT,AUTO_INCREMENT=27;
--
-- AUTO_INCREMENT for table `products`
--
ALTER TABLE `products`
  MODIFY `products_id` int(11) NOT NULL AUTO_INCREMENT,AUTO_INCREMENT=4624;
--
-- AUTO_INCREMENT for table `products_description`
--
ALTER TABLE `products_description`
  MODIFY `products_id` int(11) NOT NULL AUTO_INCREMENT,AUTO_INCREMENT=4624;
--
-- AUTO_INCREMENT for table `zones`
--
ALTER TABLE `zones`
  MODIFY `zone_id` int(11) NOT NULL AUTO_INCREMENT,AUTO_INCREMENT=66;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
