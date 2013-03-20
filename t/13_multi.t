use strict;
use Test::More;
use Test::riak;

TODO: {
    todo_skip "Have not supported multiple instances yet.", 1;
    my $riak1 = Test::riak->new or plan 'skip_all' => $Test::riak::errstr;
    my $riak2 = Test::riak->new or plan 'skip_all' => $Test::riak::errstr;

    isnt($riak1->pb_port, $riak2->pb_port, 'dfferent pb port');
    isnt($riak1->http_port, $riak2->http_port, 'dfferent http port');
}

done_testing;
