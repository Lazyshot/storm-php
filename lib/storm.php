<?php

class Tuple {
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
class Storm {
	public $anchor_tuple = null;
	
	protected function readStringMsg(){
		global $fh;

		$msg = "";

		while(true)
		{
			$line = trim(fgets(STDIN));	
			if($line == "end")
				break;

			$msg = $msg . $line . "\n";
		}


		return $msg;
	}

	protected function readMsg()
	{
		return json_decode($this->readStringMsg(), true);
	}

	protected function sendToParent($s)
	{
		echo $s . "\n";
		echo "end\n";
		fflush(STDOUT);
	}

	protected function sync()
	{
		echo "sync\n";
		fflush(STDOUT);
	}

	protected function sendpid($heartbeatdir)
	{
		$pid = getmypid();
		echo $pid . "\n";
		fflush(STDOUT);
		@fclose(@fopen($heartbeatdir . "/" . $pid, "w"));
	}

	protected function sendMsgToParent($amap)
	{
		$this->sendToParent(json_encode($amap));
	}

	protected function emittuple($tup, $stream = null, $anchors = array(), $directTask = null)
	{
		if($this->anchor_tuple !== null)
			$anchors = array($this->anchor_tuple);

		$m = array('command' => 'emit');

		if($stream !== null)
			$m['stream'] = $stream;

		$m['anchors'] = array_map(function($a){ return $a->id; }, $anchors);

		if($directTask !== null)
			$m['task'] = $directTask;

		$m['tuple'] = $tup;

		$this->sendMsgToParent($m);

	}


	protected function emit($tup, $stream = null, $anchors = array())
	{
		$this->emittuple($tup, $stream, $anchors);
		return $this->readMsg();
	}

	protected function emitDirect($task, $tup, $stream, $anchors)
	{
		$this->emittuple($tup, $stream, $anchors, $task);
	}

	protected function ack($tup)
	{
		$this->sendMsgToParent(array('command' => 'ack', 'id' => $tup->id));
	}

	protected function fail($tup)
	{
		$this->sendMsgToParent(array('command' => 'fail', 'id' => $tup->id));
	}

	protected function logToParent($msg)
	{
		$this->sendMsgToParent(array('command' => 'log', 'msg' => $msg));
	}

	protected function readenv() 
	{
		$conf = $this->readMsg();
		$context = $this->readMsg();
		return array($conf, $context);
	}

	protected function readtuple()
	{
		$tupmap = $this->readMsg();
		return new Tuple($tupmap['id'], $tupmap['comp'], $tupmap['stream'], $tupmap['task'], $tupmap['tuple']);
	}

	protected function initbolt(){
		$heartbeatdir = $this->readStringMsg();
		$this->sendpid($heartbeatdir);
		return $this->readenv();
	}
	
}


class Bolt extends Storm{
	
	public function initialize($stormconf, $context)
	{
		return;
	}
	
	public function process($tuple)
	{
		return;
	}
	
	public function run()
	{
		list($conf, $context) = $this->initbolt();
		$this->initialize($conf, $context);
		try {
			while(true)
			{
				$tup = $this->readtuple();
				$this->process($tuple);
				$this->sync();
			}
		} 
		catch(Exception $e)
		{
			$this->logToParent($e->getTraceAsSTring());
		}
	}
}

class BasicBolt extends Bolt{
	public function run()
	{
		
		list($conf, $context) = $this->initbolt();
		$this->initialize($conf, $context);
		
		
		try {
			while(true)
			{
				$tup = $this->readtuple();
				$this->anchor_tuple = $tup;
				$this->process($tup);
				$this->ack($tup);
				$this->sync();
			}
		} 
		catch(Exception $e)
		{
			$this->logToParent($e->getTraceAsSTring());
		}
		
	}
}   

class Spout extends Storm{
	public function __construct(){return;}
}