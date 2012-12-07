<?php

require_once 'storm/storm_types.php';
require_once 'storm/Nimbus.php';

interface iShellBolt {}
interface iShellSpout {}

class Tuple
{
	public $id, $component, $stream, $task, $values;

	public function __construct($id, $component, $stream, $task, $values)
	{
		$this->id = $id;
		$this->component = $component;
		$this->stream = $stream;
		$this->task = $task;
		$this->values = $values;
	}
}

abstract class BaseShellComponent
{
	protected $pid;
	protected $stormConf;
	protected $topologyContext;

	protected $stormInc = null;
	protected $_DEBUG = false;

	public function __construct($debug = false)
	{
		$this->pid = getmypid();
		$this->sendCommand(array( "pid" => $this->pid ));

		$this->_debug = $debug;

		if ($this->_DEBUG)
		{
			$this->stormInc = fopen('/tmp/' . $this->pid . "_" . strtolower($_SERVER['argv'][0]) . '.txt', 'w+');
		}

		$pidDir = $this->readLine();
		@fclose(@fopen($pidDir . "/" . $this->pid, "w"));

		$this->stormConf = $this->parseMessage( $this->waitForMessage() );
		$this->topologyContext = $this->parseMessage( $this->waitForMessage() );
	}

	protected function readLine()
	{
		$line = trim(fgets(STDIN));

		if ($this->_DEBUG)
		{
			fputs($this->stormInc, $line . "\n");
		}

		return $line;
	}

	protected function waitForMessage()
	{
		$message = '';
		while (true)
		{
			$line = trim($this->readLine());

			if (strlen($line) == 0)
			{
				continue;
			}
			else if ($line == 'end')
			{
				break;
			}
			else if ($line == 'sync')
			{
				$message = '';
				continue;
			}

			$message .= $line . "\n";
		}

		return trim($message);
	}

	protected function sendCommand(array $command)
	{
		$this->sendMessage(json_encode($command));
	}

	protected function sendLog($message)
	{
		return $this->sendCommand(array(
			'command' => 'log',
			'msg' => $message
		));
	}

	protected function parseMessage($message)
	{
		$msg = json_decode($message, true);

		if ($msg)
		{
			return $msg;
		}
		else
		{
			return $message;
		}
	}

	protected function sendMessage($message)
	{
		echo $message . "\n";
		echo "end\n";
		fflush(STDOUT);
	}

}

abstract class ShellBolt extends BaseShellComponent implements iShellBolt {

	public $anchor_tuple = null;

	public function __construct()
	{
		parent::__construct();

		$this->init($this->stormConf, $this->topologyContext);
	}

	public function run()
	{
		try {
			while(true)
			{
				$command = $this->parseMessage( $this->waitForMessage() );

				if (is_array($command))
				{
					if (isset($command['tuple']))
					{
						$tupleMap = array_merge(array(
							'id' => null,
							'comp' => null,
							'stream' => null,
							'task' => null,
							'tuple' => null
						),

						$command);

						$tuple = new Tuple($tupleMap['id'], $tupleMap['comp'], $tupleMap['stream'], $tupleMap['task'], $tupleMap['tuple']);

						$this->process($tuple);

						$this->sync();
					}
				}
			}
		}
		catch(Exception $e)
		{
			$this->sendLog( $e->getTraceAsSTring() );
		}
	}

	abstract protected function process(Tuple $tuple);

	protected function init($conf, $topology)
	{
		return;
	}

	protected function sync()
	{
		$this->sendCommand(array( 'command' => 'sync' ));
	}

	protected function emitTuple(array $tuple, $stream = null, $anchors = array(), $directTask = null)
	{
		if ($this->anchor_tuple !== null)
		{
			$anchors = array($this->anchor_tuple);
		}

		$command = array(
			'command' => 'emit'
		);

		if($stream !== null)
		{
			$command['stream'] = $stream;
		}

		$command['anchors'] = array_map(function($a) {
			return $a->id;
		}, $anchors);

		if($directTask !== null)
		{
			$command['task'] = $directTask;
		}

		$command['tuple'] = $tuple;

		$this->sendCommand($command);
	}

	protected function emit($tuple, $stream = null, $anchors = array())
	{
		$this->emitTuple($tuple, $stream, $anchors);
	}

	protected function emitDirect($directTask, $tuple, $stream = null, $anchors = array())
	{
		$this->emitTuple($tuple, $stream, $anchors, $directTask);
	}

	protected function ack(Tuple $tuple)
	{
		$command = array(
			'command' => 'ack',
			'id' => $tuple->id
		);

		$this->sendCommand($command);
	}

	protected function fail(Tuple $tuple)
	{
		$command = array(
			'command' => 'fail',
			'id' => $tuple->id
		);

		$this->sendCommand($command);
	}
}

abstract class BasicBolt extends ShellBolt
{
	public function run()
	{
		try {
			while(true)
			{
				$command = $this->parseMessage( $this->waitForMessage() );

				if (is_array($command))
				{
					if (isset($command['tuple']))
					{
						$tupleMap = array_merge(array(
							'id' => null,
							'comp' => null,
							'stream' => null,
							'task' => null,
							'tuple' => null
						),

						$command);

						$tuple = new Tuple($tupleMap['id'], $tupleMap['comp'], $tupleMap['stream'], $tupleMap['task'], $tupleMap['tuple']);

						$this->anchor_tuple = $tuple;

						try
						{
							$processed = $this->process($tuple);

							$this->ack($tuple);
						}
						catch (BoltProcessException $e)
						{
							$this->fail($tuple);
						}

						$this->sync();
					}
				}
			}
		}
		catch(Exception $e)
		{
			$this->sendLog($e->getTraceAsSTring());
		}

	}
}

abstract class ShellSpout extends BaseShellComponent implements iShellSpout
{
	protected $tuples = array();

	public function __construct()
	{
		parent::__construct();

		$this->init($this->stormConf, $this->topologyContext);
	}


	abstract protected function nextTuple();
	abstract protected function ack($tuple_id);
	abstract protected function fail($tuple_id);

	public function run()
	{
		while (true)
		{
			$command = $this->parseMessage( $this->waitForMessage() );

			if (is_array($command))
			{
				if (isset($command['command']))
				{
					if ($command['command'] == 'ack')
					{
						$this->ack($command['id']);
					}
					else if ($command['command'] == 'fail')
					{
						$this->fail($command['id']);
					}
					else if ($command['command'] == 'next')
					{
						$this->nextTuple();
					}
				}
			}
		}
	}

	protected function init($stormConf, $topologyContext)
	{
		return;
	}

	final protected function emit(array $tuple, $messageId = null, $streamId = null)
	{
		return $this->emitTuple($tuple, $messageId, $streamId, null);
	}

	final protected function emitDirect($directTask, array $tuple, $messageId = null, $streamId = null)
	{
		return $this->emitTuple($tuple, $messageId, $streamId, $directTask);
	}

	final private function emitTuple(array $tuple, $messageId = null, $streamId = null, $directTask = null)
	{
		$command = array(
			'command' => 'emit'
		);

		if ($messageId !== null)
		{
			$command['id'] = $messageId;
		}

		if ($streamId !== null)
		{
			$command['stream'] = $streamId;
		}

		if ($directTask !== null)
		{
			$command['task'] = $directTask;
		}

		$command['tuple'] = $tuple;

		return $this->sendCommand($command);
	}
}

class BoltProcessException extends Exception {}

class TopologyBuilder {
	private $client;
	private $socket;
	private $transport;
	private $protocol;

	private $conf = array();

	private $spouts = array();
	private $bolts = array();
	private $state_spouts = array();

	public function __construct($host = "localhost", $port = 6627) {
		$this->socket = new TSocket($host, $port);
		$this->transport = new TBufferedTransport($this->socket);
		$this->protocol = new TBinaryProtocol($this->transport);
		$this->client = new NimbusClient($this->protocol);
	}

	public function setConf($name, $value) {
		$conf[$name] = $value;
	}

	public function addSpout($name, $file, $parallelism_hint = false) {
		$this->spouts[$name] = array(
			'spout_object' => new ShellComponent(array('execution_command' => 'php', 'script' => $file)),
			'common' => array('inputs' => array(), 'streams' => array())
		);

		if ($parallelism_hint !== false)
			$this->spouts[$name]['common']['parallelism_hint'] = $parallelism_hint;
	}

	public function addBolt($name, $file, $parallelism_hint = false) {
		$this->bolts[$name] = array(
			'bolt_object' => new ShellComponent(array(
				'execution_command' => 'php',
				'script' => $file)
			),
			'common' => array('inputs' => array(), 'streams' => array())
		);
	}

	public function addSpoutStream($from, $to, array $fields, $direct = false) {
		$this->spouts[$name]['common']['streams'][$to] = new StreamInfo(array('fields' => $fields, 'direct' => $direct));
	}

	public function addBoltStream($from, $to, array $fields, $direct = false) {
		$this->bolts[$name]['common']['streams'][$to] = new StreamInfo(array('fields' => $fields, 'direct' => $direct));
	}

}
