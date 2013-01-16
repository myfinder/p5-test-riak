use strict;
use Test::More tests => 2;
use Test::riak;

my $riak1 = Test::riak->new(auto_start => 0) or plan 'skip_all' => $Test::riak::errstr;
my $riak2 = Test::riak->new(auto_start => 0) or plan 'skip_all' => $Test::riak::errstr;

isnt($riak1->pb_port, $riak2->pb_port, 'dfferent pb port');
isnt($riak1->http_port, $riak2->http_port, 'dfferent http port');
