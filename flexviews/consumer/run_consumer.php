<?php
ini_set('output_buffering',false);
if (is_resource(STDIN)) fclose(STDIN);
require_once('include/flexcdc.php');
require_once('Console/Getopt.php');
require_once 'log4php/Logger.php';
Logger::configure('log4php_cfg.xml');
$logger = Logger::getLogger("main");
declare(ticks = 1);

$current_date = date('Y-m-d H:i:s');
$logger->info("Starting run_consumer.php");

$HOME=getenv("HOME");
$logger->info('HOME: '.$HOME);

if (function_exists('pcntl_signal')) {
	pcntl_signal(SIGTERM, "sig_handler");
	pcntl_signal(SIGHUP,  "sig_handler");
}

function sig_handler($signo)
{
     switch ($signo) {
         case SIGTERM:
         case SIGHUP:
	     die1(0);
     }
}

#
#if(!function_exists('pcntl_fork')) {
#	function pcntl_fork() {
#		die("The --daemon option requires the pcntl extension.\n");
#	}
#}

function &get_commandline() {

  $cg = new Console_Getopt();
  $args = $cg->readPHPArgv();
  array_shift($args);

  $shortOpts = 'h::v::';
  $longOpts  = array('ini=', 'help==', 'pid=', 'daemon==' );

  $params = $cg->getopt2($args, $shortOpts, $longOpts);
  if (PEAR::isError($params)) {
      $logger->fatal('Error: ' . $params->getMessage());
      exit(1);
  }
  $new_params = array();
  foreach ($params[0] as $param) {
          $param[0] = str_replace('--','', $param[0]);
          $new_params[$param[0]] = $param[1];
  }
  unset($params);

  return $new_params;
}

$params = get_commandline();
$settings = false;

#support specifying location of .ini file on command line
if(!empty($params['ini'])) {
	$settings = @parse_ini_file($params['ini'], true);
}

if(in_array('daemon', array_keys($params))) {
	if (is_resource(STDERR)) fclose(STDERR);
	if (is_resource(STDOUT)) fclose(STDOUT);
	$pid = pcntl_fork();
	if($pid == -1) {
		die('Could not fork a new process!\n');
	} elseif($pid == 0) {
		#we are now in a child process, and the capture_changes
	        #below will be daemonized
		pcntl_signal(SIGTERM, "sig_handler");
		pcntl_signal(SIGHUP,  "sig_handler");

	} else {
		#return control to the shell
		exit(0);
	}
}

#support pid file
if(!empty($params['pid'])) {
	if(file_exists($params['pid'])) {
		$pid = trim(file_get_contents($params['pid']));

		$ps = `ps -p$pid`;

		if(preg_match('/php/i',$ps)) {
			$logger->info("Already running!");
			exit(1000);
		} else {
			$logger->info("Stale lockfile detected.");
		}
	}
	file_put_contents($params['pid'], getmypid());
}

$cdc = new FlexCDC($settings);

#capture changes forever (-1):
$cdc->capture_changes(-1);
