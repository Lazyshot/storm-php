<?php

require_once(dirname(__FILE__) . '/../../lib/storm.php');

class RandomSentenceSpout extends ShellSpout
{
	protected $sentences = array(
		"the cow jumped over the moon",
		"an apple a day keeps the doctor away",
		"four score and seven years ago",
		"snow white and the seven dwarfs",
		"i am at two with nature",
	);
	
	protected function nextTuple()
	{
		sleep(.1);
		
		$sentence = $this->sentences[ rand(0, count($this->sentences) -1)];	
		$this->emit(array($sentence));
	}
	
	protected function ack($tuple_id)
	{
		return;
	}
	
	protected function fail($tuple_id)
	{
		return;
	}	
}

$SentenceSpout = new RandomSentenceSpout();
$SentenceSpout->run();