<?php

require_once('../../lib/storm.php');


class SplitSentenceBolt extends BasicBolt
{
	public function process($tuple)
	{
		$words = explode(" ", $tuple->values[0]);
		foreach($words as $word)
		{
			$this->emit(array($word));
		}
	}
	
}

$splitsentence = new SplitSentenceBolt();
$splitsentence->run();