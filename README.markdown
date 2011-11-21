Description
====

Storm-PHP 
---------

An implementation of the multi-language support for storm (github.com/nathanmarz/storm).


Version
=======

version 0.01


Examples
========


# require_once 'storm.php';
#
# class SplitSentenceBolt extends BasicBolt {
# 	public function process($tuple)
#	{
#		$words = explode(' ', $tuple->values[0]);
#
#		foreach($words as $word)
#			$this->emit(array($word));
#	}
# }
#
# $splitsentence = new SplitSentenceBolt();
# $splitsentence->run();


Author
======

Bryan Peterson - @lazyshot