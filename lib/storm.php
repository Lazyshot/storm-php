<?php

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

abstract class ShellComponent
{
	protected $pid;
	protected $stormConf;
	protected $topologyContext;
	
	public function __construct()
	{
		$this->pid = getmypid();
		$this->sendLine($this->pid);
		
		$pidDir = $this->readLine();		
		@fclose(@fopen($pidDir . "/" . $this->pid, "w"));
		
		$this->stormConf = $this->parseMessage( $this->waitForMessage() );
		$this->topologyContext = $this->parseMessage( $this->waitForMessage() );
	}
	
	protected function readLine()
	{
		$line = trim(fgets(STDIN));
		
		return $line;
	}
	
	protected function waitForMessage()
	{
		$message = '';
		while (true)
		{
			$line = $this->readLine();
			
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
			else if ($line == 'next')
			{
				$message = '';
				$this->nextTuple();
				continue;
			}
			
			$message .= $line . "\n";			
		}
		
		return $message;
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
		return json_decode($message, true);
	}
	
	protected function sendMessage($message)
	{
		echo $message . "\n";
		echo "end\n";
		fflush(STDOUT);	
	}
	
	protected function sendLine($string)
	{
		echo $string . "\n";
		fflush(STDOUT);
	}
}

abstract class ShellBolt extends ShellComponent {

	public $anchor_tuple = null;
	
	public function __construct()
	{
		parent::__construct();
		
		$this->init($this->stormConf, $this->topologyContext);
	}
	
	abstract public function run();
	abstract protected function process(Tuple $tuple);
	
	protected function init($conf, $topology)
	{
		return;
	}
		
	protected function sync()
	{
		$this->sendLine('sync');
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
		return $this->readMsg();
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

	protected function getNextTuple()
	{
		$tupleMap = $this->parseMessage( $this->waitForMessage() );
		return new Tuple($tupleMap['id'], $tupleMap['comp'], $tupleMap['stream'], $tupleMap['task'], $tupleMap['tuple']);
	}
}


class Bolt extends ShellBolt
{		
	public function run()
	{
		try {
			while(true)
			{
				$tuple = $this->getNextTuple();
				
				$this->process($tuple);
				
				$this->sync();
			}
		} 
		catch(Exception $e)
		{
			$this->sendLog( $e->getTraceAsSTring() );
		}
	}
	
	protected function process(Tuple $tuple)
	{
		return;
	}	
}

class BasicBolt extends Bolt
{
	public function run()
	{
		try {
			while(true)
			{
				$tuple = $this->getNextTuple();
				
				$this->anchor_tuple = $tuple;
				
				$this->process($tuple);	
				$this->ack($tuple);
				
				$this->sync();
			}
		} 
		catch(Exception $e)
		{
			$this->sendLog($e->getTraceAsSTring());
		}
		
	}
}

abstract class ShellSpout extends ShellComponent
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
			
			if ($command['command'] == 'ack')
			{
				$this->ack($command['id']);
			}
			else if ($command['command'] == 'fail')
			{
				$this->fail($command['id']);
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